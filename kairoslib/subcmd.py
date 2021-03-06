#!/usr/bin/env python2

import logging
import multiprocessing as mp
import multiprocessing.queues
import xml.dom.minidom

from arch_temp_data import ArchTempData
from datetime import datetime, timedelta
from host_conn import HostConn
from kairoslib.kairos_aptr import AppKairosAPTR
from mongodbcluster import MongoDBCluster
from ontap import ClusterSession, Snapshot, FlexClone, InitiatorGroup, Lun, Volume
from psutil import Process
from pymongo import MongoClient, errors
from recover_consumer import RecoverConsumer
from sys import exit
from time import sleep, time


class SubCmdMongodb:
    def __init__(self, mdbcluster_spec=None):
        if mdbcluster_spec is not None:
            for mdb_key in mdbcluster_spec.keys():
                if mdbcluster_spec[mdb_key] is None:
                    mdbcluster_spec.pop(mdb_key)

            self.mdb_spec = mdbcluster_spec

    def add(self, kdb_session=None, kdb_collection=None):
        if type(self._testing_conn()) is not bool:
            logging.error('Cannot connect to MongoDB Cluster.')
            exit(1)
        else:
            try:
                collection = kdb_session[kdb_collection]
                collection.insert_one(self.mdb_spec).inserted_id
            except errors.DuplicateKeyError, e:
                logging.error(e.message)
                exit(1)

    def remove(self, kdb_session=None, kdb_collection=None):
        collection = kdb_session[kdb_collection]
        collection.delete_one({'cluster-name': self.mdb_spec['cluster-name']})

    @staticmethod
    def list(kdb_session=None, kdb_collection=None):
        collection = kdb_session[kdb_collection]
        result = collection.find()
        return result

    def _testing_conn(self):
        try:
            mdbcluster = MongoClient(self.mdb_spec['mongodb-uri'])
            test_conn = mdbcluster.is_mongos
            if type(test_conn) == bool:
                return True
            else:
                return False
        except errors.ServerSelectionTimeoutError, e:
            logging.error(e.message)
            exit(1)


class SubCmdNetapp:
    def __init__(self, ntapsys_spec=None):
        if ntapsys_spec is not None:
            for ntapsys_key in ntapsys_spec.keys():
                if ntapsys_spec[ntapsys_key] is None:
                    ntapsys_spec.pop(ntapsys_key)

            self.netappsys = ntapsys_spec

    def add(self, kdb_session, kdb_collection):
        verify_netapp = self._testing_conn()
        if type(verify_netapp) is tuple:
            logging.error('Failed to connect to ' + self.ndp_spec['netapp-ip'])
            exit(1)
        elif (type(verify_netapp) is list) or (type(verify_netapp) is str):
            collection = kdb_session[kdb_collection]
            try:
                collection.insert_one(self.netappsys).inserted_id
                logging.info('NetApp system ' + self.netappsys['netapp-ip'] + ' has been added to the database.')
            except errors.DuplicateKeyError, e:
                logging.error(e.message)
                exit(1)

    def remove(self, kdb_session, kdb_collection):
        collection = kdb_session[kdb_collection]
        try:
            result = collection.delete_one({'netapp-ip': self.netappsys['netapp-ip']}).deleted_count
            return result
        except errors, e:
            logging.error(e.message)
            exit(1)

    def list(self, kdb_session, kdb_collection):
        collection = kdb_session[kdb_collection]
        result = collection.find()
        return result

    def _testing_conn(self):
        if 'svm-name' not in self.netappsys:
            ns = ClusterSession(self.netappsys['netapp-ip'], self.netappsys['username'], self.netappsys['password'])
            return ns.get_nodes()
        else:
            ns = ClusterSession(self.netappsys['netapp-ip'], self.netappsys['username'], self.netappsys['password'],
                                self.netappsys['svm-name'])
            return ns.get_vserver()


class SubCmdBackup:
    def __init__(self, bkp_spec=None):
        if bkp_spec is not None:
            self.backup = bkp_spec

    def create(self, kdb_session):
        try:
            kdb_mdbclusters = kdb_session['mdbclusters']
            cluster_info = kdb_mdbclusters.find_one({'cluster-name': self.backup['cluster-name']})

            if cluster_info is None:
                logging.error('MongoDB cluster ' + self.backup['cluster-name'] + ' not found.')
                exit(1)
            logging.info('Found ' + self.backup['cluster-name'] + ' on Kairos repository.')
        except errors.ConnectionFailure or errors.CursorNotFound, e:
            logging.error(e.message)
            exit(1)

        if cluster_info['mongodb-auth'] == 'off':
            mdbcluster = MongoDBCluster(cluster_info['mongodb-uri'])
        else:
            pass

        # -- backup metadata data structure
        bkp_metadata = dict()

        # -- Getting MongoDB cluster topology
        topology = mdbcluster.get_topology()
        logging.info(self.backup['cluster-name'] + ' is a ' + topology['cluster_type'] + ' cluster.')

        if topology['cluster_type'] == 'replSet':
            for rs_member in topology['members']:
                if rs_member['stateStr'] == 'PRIMARY' or rs_member['stateStr'] == 'SECONDARY':
                    host = HostConn(ipaddr=rs_member['name'].split(':')[0], username=self.backup['username'])
                    rs_member['storage_info'] = host.get_storage_layout(cluster_info['mongodb-mongod-conf'])
                    host.close()
                    logging.info('Collecting info about host {}'.format(rs_member['name'].split(':')[0]))

        elif topology['cluster_type'] == 'sharded':
            for cs_member in topology['config_servers']:
                if cs_member['stateStr'] == 'PRIMARY' or cs_member['stateStr'] == 'SECONDARY':
                    host = HostConn(ipaddr=cs_member['name'].split(':')[0], username=self.backup['username'])
                    cs_member['storage_info'] = host.get_storage_layout(cluster_info['mongodb-mongod-conf'])
                    host.close()
                    logging.info('Collecting info about config server {}'.format(cs_member['name'].split(':')[0]))
            for shard_replset in topology['shards']:
                for shard_member in shard_replset['shard_members']:
                    if shard_member['stateStr'] == 'PRIMARY' or shard_member['stateStr'] == 'SECONDARY':
                        host = HostConn(ipaddr=shard_member['name'].split(':')[0], username=self.backup['username'])
                        shard_member['storage_info'] = host.get_storage_layout(cluster_info['mongodb-mongod-conf'])
                        host.close()
                        logging.info('Collecting info about shard member {}'.format(shard_member['name'].split(':')[0]))

        snapshot_list = list()
        if topology['cluster_type'] == 'replSet':
            for rs_member in topology['members']:
                if rs_member['stateStr'] == 'PRIMARY' or rs_member['stateStr'] == 'SECONDARY':
                    per_server_cg = dict()
                    per_server_cg['volume'] = list()
                    per_server_cg['snapname'] = self.backup['backup-name']
                    per_server_cg['snap-type'] = 'cgsnap'
                    per_server_cg['cg-timeout'] = 'relaxed'
                    per_server_cg['member_name'] = rs_member['name'].split(':')[0]
                    for volume in rs_member['storage_info']['volume_topology']:
                        per_server_cg['volume'].append(volume['volume'])
                        per_server_cg['svm-name'] = volume['svm-name']

                    snapshot_list.append(per_server_cg)

        elif topology['cluster_type'] == 'sharded':
            for cs_member in topology['config_servers']:
                if cs_member['stateStr'] == 'PRIMARY' or cs_member['stateStr'] == 'SECONDARY':
                    per_server_cg = dict()
                    per_server_cg['volume'] = list()
                    per_server_cg['snapname'] = self.backup['backup-name']
                    per_server_cg['snap-type'] = 'cgsnap'
                    per_server_cg['cg-timeout'] = 'relaxed'
                    per_server_cg['member_name'] = cs_member['name'].split(':')[0]
                    for volume in cs_member['storage_info']['volume_topology']:
                        per_server_cg['volume'].append(volume['volume'])
                        per_server_cg['svm-name'] = volume['svm-name']

                    snapshot_list.append(per_server_cg)

            for shard_replset in topology['shards']:
                for shard_member in shard_replset['shard_members']:
                    if shard_member['stateStr'] == 'PRIMARY' or shard_member['stateStr'] == 'SECONDARY':
                        per_server_cg = dict()
                        per_server_cg['volume'] = list()
                        per_server_cg['snapname'] = self.backup['backup-name']
                        per_server_cg['snap-type'] = 'cgsnap'
                        per_server_cg['cg-timeout'] = 'relaxed'
                        per_server_cg['member_name'] = shard_member['name'].split(':')[0]
                        for volume in shard_member['storage_info']['volume_topology']:
                            per_server_cg['volume'].append(volume['volume'])
                            per_server_cg['svm-name'] = volume['svm-name']

                        snapshot_list.append(per_server_cg)

        # -- If sharded cluster, stopping the balancer before taking any snapshot
        if topology['cluster_type'] == 'sharded':
            mdbcluster.stop_balancer()

        # -- Creating CG snapshots
        # -- -- Connecting to kairos_repo to get the storage credentials
        kdb_netapp = kdb_session['ntapsystems']

        for cgsnapshot in snapshot_list:
            svm_info = kdb_netapp.find_one({'svm-name': cgsnapshot['svm-name']})
            cs_svm = ClusterSession(svm_info['netapp-ip'], svm_info['username'], svm_info['password'], svm_info['svm-name'])
            cgsnap = Snapshot(cgsnapshot)
            result = cgsnap.cgcreate(cs_svm)
            if result[0] == 'passed':
                logging.info('CG Snapshot of member {} has been successfully taken.'.format(cgsnapshot['member_name']))
            else:
                #TODO: Rollback backup operation by deleting other volumes snapshots
                logging.error('CG Snapshot of member {} has failed.'.format(cgsnapshot['member_name']))
                logging.error(result[1])
                if topology['cluster_type'] == 'sharded':
                    mdbcluster.start_balancer()
                exit(1)

        # -- If sharded cluster, starting the balancer after taking a snapshot
        if topology['cluster_type'] == 'sharded':
            mdbcluster.start_balancer()

        # -- Saving backup metadata to the repository database
        bkp_metadata['created_at'] = datetime.now()
        bkp_metadata['backup_name'] = self.backup['backup-name']
        bkp_metadata['cluster_name'] = self.backup['cluster-name']
        bkp_metadata['mongo_topology'] = topology
        bkp_metadata['retention'] = self._calc_retention(self.backup['retention'], bkp_metadata['created_at'])

        kdb_backups = kdb_session['backups']
        kdb_backups.insert_one(bkp_metadata)

    def delete(self, kdb_session):
        kdb_bkps = kdb_session['backups']
        bkp2delete = kdb_bkps.find_one({'backup_name': self.backup['backup-name']})

        if bkp2delete is None:
            logging.error('Backup ' + self.backup['backup-name'] + ' not found.')
            exit(1)

        delete_list = dict()
        if bkp2delete['mongo_topology']['cluster_type'] == 'replSet':
            for rs_member in bkp2delete['mongo_topology']['members']:
                if rs_member['stateStr'] == 'PRIMARY' or rs_member['stateStr'] == 'SECONDARY':
                    for vol in rs_member['storage_info']['volume_topology']:
                        if vol['svm-name'] not in delete_list.keys():
                            delete_list[vol['svm-name']] = list()
                            delete_list[vol['svm-name']].append(vol['volume'])
                        else:
                            delete_list[vol['svm-name']].append(vol['volume'])

        elif bkp2delete['mongo_topology']['cluster_type'] == 'sharded':
            for cs_member in bkp2delete['mongo_topology']['config_servers']:
                if cs_member['stateStr'] == 'PRIMARY' or cs_member['stateStr'] == 'SECONDARY':
                    for vol in cs_member['storage_info']['volume_topology']:
                        if vol['svm-name'] not in delete_list.keys():
                            delete_list[vol['svm-name']] = list()
                            delete_list[vol['svm-name']].append(vol['volume'])
                        else:
                            delete_list[vol['svm-name']].append(vol['volume'])

            for shard_replset in bkp2delete['mongo_topology']['shards']:
                for shard_member in shard_replset['shard_members']:
                    if shard_member['stateStr'] == 'PRIMARY' or shard_member['stateStr'] == 'SECONDARY':
                        for vol in shard_member['storage_info']['volume_topology']:
                            if vol['svm-name'] not in delete_list.keys():
                                delete_list[vol['svm-name']] = list()
                                delete_list[vol['svm-name']].append(vol['volume'])
                            else:
                                delete_list[vol['svm-name']].append(vol['volume'])

        # -- Checking if the snapshot is ready to be deleted across all volumes
        kdb_netapp = kdb_session['ntapsystems']
        for svm in delete_list.keys():
            svm_info = kdb_netapp.find_one({'svm-name': svm})
            cs_svm = ClusterSession(svm_info['netapp-ip'], svm_info['username'], svm_info['password'],
                                    svm_info['svm-name'])
            for volume in delete_list[svm]:
                snapspec = dict()
                snapspec['volume'] = volume
                snapspec['snapname'] = bkp2delete['backup_name']
                snapshot = Snapshot(snapspec)
                result_getsnap = snapshot.get_snaps(cs_svm)
                if result_getsnap[0] == 'passed':
                    xmloutput = xml.dom.minidom.parseString(result_getsnap[1])
                    snap_busy = xmloutput.getElementsByTagName('busy')[0].firstChild.data

                    if snap_busy == 'false':
                        logging.info('Snapshot ' + snapspec['snapname'] + ' from volume ' + snapspec['volume'] +
                                     ' passed the inspection to be deleted.')
                    else:
                        logging.error('Snapshot ' + snapspec['snapname'] + ' from volume ' + snapspec['volume'] +
                                      ' is busy and cannot be deleted.')
                        exit(1)

        # -- Deleting snapshot across all volumes
        kdb_netapp = kdb_session['ntapsystems']
        for svm in delete_list.keys():
            svm_info = kdb_netapp.find_one({'svm-name': svm})
            cs_svm = ClusterSession(svm_info['netapp-ip'], svm_info['username'], svm_info['password'],
                                    svm_info['svm-name'])
            for volume in delete_list[svm]:
                snapspec = dict()
                snapspec['volume'] = volume
                snapspec['snapname'] = bkp2delete['backup_name']
                snapshot = Snapshot(snapspec)
                delete_result = snapshot.delete(cs_svm)
                if delete_result[0] == 'passed':
                    logging.info('Snapshot ' + snapspec['snapname'] + ' has been deleted from volume ' + snapspec['volume'])
                else:
                    logging.error('Failed to delete snapshot ' + snapspec['snapname'] + ' on volume ' + snapspec['volume'] + '.')

        result_bkp2delete = kdb_bkps.delete_one({'backup_name': self.backup['backup-name']})

    def list_all(self, kdb_session):
        kdb_bkps = kdb_session['backups']
        result = kdb_bkps.find()
        print '{:30} \t {:30} {:30}'.format('Backup Name', 'Created At', 'Expires At')
        for bkp in result:
            print '{:30} \t {:30} {:30}'.format(bkp['backup_name'], bkp['created_at'].strftime('%Y-%m-%d %H:%M:%S.%f'),
                                                bkp['retention'].strftime('%Y-%m-%d %H:%M:%S.%f'))

    def search_for_db(self, kdb_session, keyword):
        kdb_bkps = kdb_session['backups']
        key_search = 'mongo_topology.shards.databases.' + keyword
        result = kdb_bkps.find({ key_search: { '$exists': True} })
        print '{:30} \t {:30} {:30}'.format('Backup Name', 'Created At', 'Retention')
        for bkp in result:
            print '{:30} \t {:30} {:30}'.format(bkp['backup_name'],
                                                bkp['created_at'].strftime('%Y-%m-%d %H:%M:%S.%f'),
                                                bkp['retention'].strftime('%Y-%m-%d %H:%M:%S.%f')
                                                )

    def search_for_collection(self, kdb_session, database, collection):
        kdb_bkps = kdb_session['backups']
        key_search = 'mongo_topology.shards.databases.' + database
        result = kdb_bkps.find({ key_search: collection })
        print '{:30} \t {:30} {:30}'.format('Backup Name', 'Created At', 'Retention')
        for bkp in result:
            print '{:30} \t {:30} {:30}'.format(bkp['backup_name'],
                                                bkp['created_at'].strftime('%Y-%m-%d %H:%M:%S.%f'),
                                                bkp['retention'].strftime('%Y-%m-%d %H:%M:%S.%f')
                                                )

    def _calc_retention(self, retention, created_at):
        unit = retention[len(retention)-1:]
        value = retention[:-1]
        if unit == 'm':
            return created_at + timedelta(minutes=int(value))
        elif unit == 'h':
            return created_at + timedelta(hours=int(value))
        elif unit == 'd':
            return created_at + timedelta(days=int(value))
        elif unit == 'w':
            return created_at + timedelta(weeks=int(value))


class SubCmdRestore:
    def __init__(self, rst_spec=None):
        self.backup_name = rst_spec['backup-name']
        self.cluster_name = rst_spec['cluster-name']
        self.username = rst_spec['username']
        self.archive_repo_uri = rst_spec['archive_repo_uri']
        self.archive_repo_name = rst_spec['archive_repo_name']

    def restore(self, catalog_sess=None):
        bkp2restore = catalog_sess.find_one(coll_name='backups', query={'backup_name': self.backup_name,
                                                                        'cluster_name': self.cluster_name})

        if bkp2restore is None:
            logging.error('Backup {} could not be found for cluster {}.'.format(self.backup_name, self.cluster_name))
            exit(1)

        # -- Preparation phase for ReplicaSet Cluster
        if bkp2restore['mongo_topology']['cluster_type'] == 'replSet':
            for rs_member in bkp2restore['mongo_topology']['members']:
                host = HostConn(ipaddr=rs_member['name'].split(':')[0], username=self.username)
                # -- Stopping mongod
                stop_mongo = host.stop_service('mongod')
                if stop_mongo[1] != 0:
                    logging.error('Cannot stop MongoDB on host {}.'.format(rs_member['name'].split(':')[0]))
                    exit(1)
                else:
                    logging.info('MongoDB has been stopped on host {}.'.format(rs_member['name'].split(':')[0]))

                # -- For every data bearing node: umount, vgchange and multipath stop
                if rs_member['stateStr'] == 'PRIMARY' or rs_member['stateStr'] == 'SECONDARY':
                    umount_fs = host.umount_fs(fs_mountpoint=rs_member['storage_info']['mountpoint'])
                    if umount_fs[1] != 0:
                        logging.error('Cannot unmount MongoDB file system {}.'.format(rs_member['storage_info']['mountpoint']))
                        exit(1)
                    else:
                        logging.info('MongoDB file system {} has been successfully unmounted.'.format(rs_member['storage_info']['mountpoint']))

                    vgchange = host.disable_vg(vg_name=rs_member['storage_info']['lvm_vgname'])
                    if vgchange[1] != 0:
                        logging.error('Cannot deactive volume group {}.'.format(rs_member['storage_info']['lvm_vgname']))
                        exit(1)
                    else:
                        logging.info('MongoDB volume group {} has been successfully disabled.'.format(rs_member['storage_info']['lvm_vgname']))

                    multipath = host.stop_service('multipathd')
                    if multipath[1] != 0:
                       logging.error('Cannot stop multipathd on host {}.'.format(rs_member['name'].split(':')[0]))
                       exit(1)
                    else:
                        logging.info('Multipathd has been successfully stopped on host {}.'.format(rs_member['name'].split(':')[0]))

        # -- Preparation phase for Sharded Clusters
        if bkp2restore['mongo_topology']['cluster_type'] == 'sharded':
            for cs_member in bkp2restore['mongo_topology']['config_servers']:
                host = HostConn(ipaddr=cs_member['name'].split(':')[0], username=self.username)
                # -- Stopping mongod
                stop_mongo = host.stop_service('mongod')
                if stop_mongo[1] != 0:
                    logging.error(
                        'Cannot stop MongoDB on host {}.'.format(cs_member['name'].split(':')[0]))
                    exit(1)
                else:
                    logging.info(
                        'MongoDB has been stopped on host {}.'.format(cs_member['name'].split(':')[0]))

                # -- For every data bearing node: umount, vgchange and multipath stop
                if cs_member['stateStr'] == 'PRIMARY' or cs_member['stateStr'] == 'SECONDARY':
                    umount_fs = host.umount_fs(fs_mountpoint=cs_member['storage_info']['mountpoint'])
                    if umount_fs[1] != 0:
                        logging.error('Cannot unmount MongoDB file system {}.'.format(
                            cs_member['storage_info']['mountpoint']))
                        exit(1)
                    else:
                        logging.info('MongoDB file system {} has been successfully unmounted.'.format(
                            cs_member['storage_info']['mountpoint']))

                    vgchange = host.disable_vg(vg_name=cs_member['storage_info']['lvm_vgname'])
                    if vgchange[1] != 0:
                        logging.error('Cannot deactive volume group {}.'.format(
                            cs_member['storage_info']['lvm_vgname']))
                        exit(1)
                    else:
                        logging.info('MongoDB volume group {} has been successfully disabled.'.format(
                            cs_member['storage_info']['lvm_vgname']))

                    multipath = host.stop_service('multipathd')
                    if multipath[1] != 0:
                        logging.error('Cannot stop multipathd on host {}.'.format(
                            cs_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info('Multipathd has been successfully stopped on host {}.'.format(
                            cs_member['name'].split(':')[0]))

            for shard_replset in bkp2restore['mongo_topology']['shards']:
                for shard_member in shard_replset['shard_members']:
                    host = HostConn(ipaddr=shard_member['name'].split(':')[0], username=self.username)
                    # -- Stopping mongod
                    stop_mongo = host.stop_service('mongod')
                    if stop_mongo[1] != 0:
                        logging.error(
                            'Cannot stop MongoDB on host {}.'.format(shard_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info(
                            'MongoDB has been stopped on host {}.'.format(shard_member['name'].split(':')[0]))

                    # -- For every data bearing node: umount, vgchange and multipath stop
                    if shard_member['stateStr'] == 'PRIMARY' or shard_member['stateStr'] == 'SECONDARY':
                        umount_fs = host.umount_fs(fs_mountpoint=shard_member['storage_info']['mountpoint'])
                        if umount_fs[1] != 0:
                            logging.error('Cannot unmount MongoDB file system {}.'.format(
                                shard_member['storage_info']['mountpoint']))
                            exit(1)
                        else:
                            logging.info('MongoDB file system {} has been successfully unmounted.'.format(
                                shard_member['storage_info']['mountpoint']))

                        vgchange = host.disable_vg(vg_name=shard_member['storage_info']['lvm_vgname'])
                        if vgchange[1] != 0:
                            logging.error('Cannot deactive volume group {}.'.format(
                                shard_member['storage_info']['lvm_vgname']))
                            exit(1)
                        else:
                            logging.info('MongoDB volume group {} has been successfully disabled.'.format(
                                shard_member['storage_info']['lvm_vgname']))

                        multipath = host.stop_service('multipathd')
                        if multipath[1] != 0:
                            logging.error('Cannot stop multipathd on host {}.'.format(
                                shard_member['name'].split(':')[0]))
                            exit(1)
                        else:
                            logging.info('Multipathd has been successfully stopped on host {}.'.format(
                                shard_member['name'].split(':')[0]))

        # -- Restore phase
        snaprestore_list = dict()
        if bkp2restore['mongo_topology']['cluster_type'] == 'replSet':
            for rs_member in bkp2restore['mongo_topology']['members']:
                if rs_member['stateStr'] == 'PRIMARY' or rs_member['stateStr'] == 'SECONDARY':
                    for vol in rs_member['storage_info']['volume_topology']:
                        if vol['svm-name'] not in snaprestore_list.keys():
                            snaprestore_list[vol['svm-name']] = list()
                            snaprestore_list[vol['svm-name']].append(vol['volume'])
                        else:
                            snaprestore_list[vol['svm-name']].append(vol['volume'])

        elif bkp2restore['mongo_topology']['cluster_type'] == 'sharded':
            for cs_member in bkp2restore['mongo_topology']['config_servers']:
                if cs_member['stateStr'] == 'PRIMARY' or cs_member['stateStr'] == 'SECONDARY':
                    for vol in cs_member['storage_info']['volume_topology']:
                        if vol['svm-name'] not in snaprestore_list.keys():
                            snaprestore_list[vol['svm-name']] = list()
                            snaprestore_list[vol['svm-name']].append(vol['volume'])
                        else:
                            snaprestore_list[vol['svm-name']].append(vol['volume'])

            for shard_replset in bkp2restore['mongo_topology']['shards']:
                for shard_member in shard_replset['shard_members']:
                    if shard_member['stateStr'] == 'PRIMARY' or shard_member[
                        'stateStr'] == 'SECONDARY':
                        for vol in shard_member['storage_info']['volume_topology']:
                            if vol['svm-name'] not in snaprestore_list.keys():
                                snaprestore_list[vol['svm-name']] = list()
                                snaprestore_list[vol['svm-name']].append(vol['volume'])
                            else:
                                snaprestore_list[vol['svm-name']].append(vol['volume'])

        for svm in snaprestore_list.keys():
            svm_info = catalog_sess.find_one(coll_name='ntapsystems', query={'svm-name': svm})
            cs_svm = ClusterSession(svm_info['netapp-ip'], svm_info['username'], svm_info['password'],
                                    svm_info['svm-name'])
            for volume in snaprestore_list[svm]:
                snapspec = dict()
                snapspec['volume'] = volume
                snapspec['snapname'] = self.backup_name
                snapshot = Snapshot(snapspec)
                restore_result = snapshot.restore(cs_svm)
                if restore_result[0] == 'passed':
                    logging.info('Snapshot ' + snapspec['snapname'] + ' has been restored on volume ' + snapspec['volume'])
                else:
                    logging.error('Failed to restore snapshot ' + snapspec['snapname'] + ' on volume ' + snapspec['volume'] + '.')
                    logging.error(restore_result[1])

        # -- Post restore phase for ReplicaSet Cluster
        if bkp2restore['mongo_topology']['cluster_type'] == 'replSet':
            for rs_member in bkp2restore['mongo_topology']['members']:
                host = HostConn(ipaddr=rs_member['name'].split(':')[0], username=self.username)
                # -- For every data bearing node: multipath start, vgchange, mount
                if rs_member['stateStr'] == 'PRIMARY' or rs_member['stateStr'] == 'SECONDARY':
                    multipath = host.start_service('multipathd')
                    if multipath[1] != 0:
                        logging.error('Cannot start multipathd on host {}.'.format(rs_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info(
                            'Multipathd has been successfully started on host {}.'.format(rs_member['name'].split(':')[0]))

                    vgchange = host.enable_vg(vg_name=rs_member['storage_info']['lvm_vgname'])
                    if vgchange[1] != 0:
                        logging.error('Cannot activate volume group {} on host {}.'.format(rs_member['storage_info']['lvm_vgname'], rs_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info('MongoDB volume group {} has been successfully activated.'.format(
                            rs_member['storage_info']['lvm_vgname']))

                    mount_fs = host.mount_fs(fs_mountpoint=rs_member['storage_info']['mountpoint'], fs_type=rs_member['storage_info']['fs_type'], device=rs_member['storage_info']['mdb_device'])
                    if mount_fs[1] != 0:
                        logging.error(
                            'Cannot mount MongoDB file system {} on host {}.'.format(rs_member['storage_info']['mountpoint'], rs_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info('MongoDB file system {} has been successfully mounted on host {}.'.format(
                            rs_member['storage_info']['mountpoint'], rs_member['name'].split(':')[0]))

                # -- Starting mongod
                start_mongo = host.start_service('mongod')
                if start_mongo[1] != 0:
                    logging.error('Cannot start MongoDB on host {}.'.format(rs_member['name'].split(':')[0]))
                    exit(1)
                else:
                    logging.info('MongoDB has been started on host {}.'.format(rs_member['name'].split(':')[0]))

        # -- Post restore phase for Sharded Clusters
        if bkp2restore['mongo_topology']['cluster_type'] == 'sharded':
            for cs_member in bkp2restore['mongo_topology']['config_servers']:
                host = HostConn(ipaddr=cs_member['name'].split(':')[0], username=self.username)
                # -- For every data bearing node: multipath start, vgchange, mount
                if cs_member['stateStr'] == 'PRIMARY' or cs_member['stateStr'] == 'SECONDARY':
                    multipath = host.start_service('multipathd')
                    if multipath[1] != 0:
                        logging.error('Cannot start multipathd on host {}.'.format(cs_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info(
                            'Multipathd has been successfully started on host {}.'.format(cs_member['name'].split(':')[0]))

                    vgchange = host.enable_vg(vg_name=cs_member['storage_info']['lvm_vgname'])
                    if vgchange[1] != 0:
                        logging.error('Cannot activate volume group {} on host {}.'.format(cs_member['storage_info']['lvm_vgname'], cs_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info('MongoDB volume group {} has been successfully activated.'.format(
                            cs_member['storage_info']['lvm_vgname']))

                    mount_fs = host.mount_fs(fs_mountpoint=cs_member['storage_info']['mountpoint'], fs_type=cs_member['storage_info']['fs_type'], device=cs_member['storage_info']['mdb_device'])
                    if mount_fs[1] != 0:
                        logging.error(
                            'Cannot mount MongoDB file system {} on host {}.'.format(cs_member['storage_info']['mountpoint'], cs_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info('MongoDB file system {} has been successfully mounted on host {}.'.format(
                            cs_member['storage_info']['mountpoint'], cs_member['name'].split(':')[0]))

                # -- Starting mongod
                start_mongo = host.start_service('mongod')
                if start_mongo[1] != 0:
                    logging.error('Cannot start MongoDB on host {}.'.format(cs_member['name'].split(':')[0]))
                    exit(1)
                else:
                    logging.info('MongoDB has been started on host {}.'.format(cs_member['name'].split(':')[0]))

            for shard_replset in bkp2restore['mongo_topology']['shards']:
                for shard_member in shard_replset['shard_members']:
                    host = HostConn(ipaddr=shard_member['name'].split(':')[0], username=self.username)
                    # -- For every data bearing node: multipath start, vgchange, mount
                    if shard_member['stateStr'] == 'PRIMARY' or shard_member['stateStr'] == 'SECONDARY':
                        multipath = host.start_service('multipathd')
                        if multipath[1] != 0:
                            logging.error('Cannot start multipathd on host {}.'.format(shard_member['name'].split(':')[0]))
                            exit(1)
                        else:
                            logging.info(
                                'Multipathd has been successfully started on host {}.'.format(
                                    shard_member['name'].split(':')[0]))

                        vgchange = host.enable_vg(vg_name=shard_member['storage_info']['lvm_vgname'])
                        if vgchange[1] != 0:
                            logging.error('Cannot activate volume group {} on host {}.'.format(
                                shard_member['storage_info']['lvm_vgname'], shard_member['name'].split(':')[0]))
                            exit(1)
                        else:
                            logging.info('MongoDB volume group {} has been successfully activated.'.format(
                                shard_member['storage_info']['lvm_vgname']))

                        mount_fs = host.mount_fs(fs_mountpoint=shard_member['storage_info']['mountpoint'],
                                                 fs_type=shard_member['storage_info']['fs_type'],
                                                 device=shard_member['storage_info']['mdb_device'])
                        if mount_fs[1] != 0:
                            logging.error(
                                'Cannot mount MongoDB file system {} on host {}.'.format(
                                    shard_member['storage_info']['mountpoint'], shard_member['name'].split(':')[0]))
                            exit(1)
                        else:
                            logging.info('MongoDB file system {} has been successfully mounted on host {}.'.format(
                                shard_member['storage_info']['mountpoint'], shard_member['name'].split(':')[0]))

                    # -- Starting mongod
                    start_mongo = host.start_service('mongod')
                    if start_mongo[1] != 0:
                        logging.error('Cannot start MongoDB on host {}.'.format(shard_member['name'].split(':')[0]))
                        exit(1)
                    else:
                        logging.info('MongoDB has been started on host {}.'.format(shard_member['name'].split(':')[0]))

        # -- Housekeeping on backup's metadata
        delete_newers = catalog_sess.remove_many(coll_name='backups', query={'created_at':
                                                                                 { '$gt': bkp2restore['created_at']}})
        # -- restore completed
        logging.info('Restore operation has been completed.')


class SubCmdClone:
    def __init__(self, clone_args=None):
        if clone_args['backup-name'] is not None:
            self.backup_name = clone_args['backup-name']
        if clone_args['cluster-name'] is not None:
            self.cluster_name = clone_args['cluster-name']
        if clone_args['desc'] is not None:
            self.desc = clone_args['desc']
        else:
            self.desc = ''
        self.clone_name = clone_args['clone-name']
        self.clone_spec = clone_args['clone-spec']
        self.username = clone_args['username']
        self.clone_uid = time()

    def create_storage_clone(self, kdb_session=None):
        kdb_backup = kdb_session['backups']
        bkp2clone = kdb_backup.find_one({'backup_name': self.backup_name, 'cluster_name': self.cluster_name})
        if bkp2clone is None:
            logging.error('Cannot find backup {} for cluster {}.'.format(self.backup_name, self.cluster_name))
            exit(1)

        # -- checking if clone spec file has the required number of members per replicaset
        if self.clone_spec['cluster_type'] == 'replicaSet':
            odd_members = len(self.clone_spec['replset']['members']) / 2.0
            if odd_members == 0:
                logging.error('You need an odd number of members to establish a replicaSet.')
                exit(1)
        elif self.clone_spec['cluster_type'] == 'sharded':
            odd_cs_members = len(self.clone_spec['config_servers']['members']) / 2.0
            if odd_cs_members == 0:
                logging.error('You need an odd number of members to establish a CSRS.')
                exit(1)

            for shard in self.clone_spec['shards']:
                odd_sh_members = len(shard['shard_members']) / 2.0
                if odd_sh_members == 0:
                    logging.error('You need an odd number of members to establish a shard.')
                    exit(1)

        # -- cloned cluster structure
        cloned_cluster = dict()
        cloned_cluster['cluster_type'] = self.clone_spec['cluster_type']
        if self.clone_spec['cluster_type'] == 'replicaSet':
            cloned_cluster['setname'] = self.clone_spec['replset']['setname']
            cloned_cluster['members'] = list()
            cloned_cluster['reconfig'] = dict()
        elif self.clone_spec['cluster_type'] == 'sharded':
            cloned_cluster['config_servers'] = dict()
            cloned_cluster['config_servers']['setname'] = self.clone_spec['config_servers']['setname']
            cloned_cluster['config_servers']['members'] = list()
            cloned_cluster['config_servers']['reconfig'] = dict()
            cloned_cluster['shards'] = list()

        # -- replicaSet and shard reconfig -- rc stands for reconfig
        # -- cc_cs stands for cloned_cluster config server
        # -- cc_sh stands for cloned_cluster shard
        if self.clone_spec['cluster_type'] == 'replicaSet':
            count = 0
            rc_members = dict()
            for spec_member in self.clone_spec['replset']['members']:
                rc_members['members.' + str(count) + '.host'] = spec_member['hostname'] + ':' + spec_member['port']
                rc_members['members.' + str(count) + '.arbiterOnly'] = spec_member['arbiter_only']
                count += 1
            cloned_cluster['reconfig'] = rc_members
        elif self.clone_spec['cluster_type'] == 'sharded':
            # -- config server reconfig
            count = 0
            rc_cs_members = dict()
            for spec_cs_member in self.clone_spec['config_servers']['members']:
                rc_cs_members['members.' + str(count) + '._id'] = count
                rc_cs_members['members.' + str(count) + '.host'] = spec_cs_member['hostname'] + ':' + \
                                                                  spec_cs_member['port']
                rc_cs_members['members.' + str(count) + '.arbiterOnly'] = spec_cs_member['arbiter_only']
                rc_cs_members['members.' + str(count) + '.buildIndexes'] = True
                rc_cs_members['members.' + str(count) + '.hidden'] = False
                rc_cs_members['members.' + str(count) + '.priority'] = 1
                rc_cs_members['members.' + str(count) + '.tags'] = dict()
                rc_cs_members['members.' + str(count) + '.slaveDelay'] = 0
                rc_cs_members['members.' + str(count) + '.votes'] = 1
                count += 1
            cloned_cluster['config_servers']['reconfig'] = rc_cs_members

        # -- Stage 1 :: Setting it up -------------------
        if self.clone_spec['cluster_type'] == 'sharded':
            for spec_cs_member in self.clone_spec['config_servers']['members']:
                if spec_cs_member['arbiter_only']:
                    cloned_cluster['config_servers']['members'].append(spec_cs_member)
                    continue

                config_server = spec_cs_member

                host = HostConn(ipaddr=spec_cs_member['hostname'], username=self.username)
                
                igroup_spec = dict()
                result_get_hostname = host.get_hostname()
                if result_get_hostname[1] != 0:
                    logging.error('Could not get hostname from host {}.'.format(spec_cs_member['hostname']))
                    exit(1)
                else:
                    logging.info('Preparing initiator group for host {}.'.format(spec_cs_member['hostname']))
                    igroup_spec['igroup-name'] = 'ig_' + result_get_hostname[0].strip('\n') + '_' + self.clone_name
                    igroup_spec['igroup-type'] = spec_cs_member['protocol']
                    igroup_spec['os-type'] = 'linux'
                    if spec_cs_member['protocol'] == 'iscsi':
                        result_get_iqn = host.get_iscsi_iqn()
                        if result_get_iqn[1] != 0:
                            logging.error('Could not get initiator name from host {}.'.format(spec_cs_member['hostname']))
                            exit(1)
                        else:
                            logging.info('Collecting initiator name on host {}.'.format(spec_cs_member['hostname']))
                            config_server['initiator'] = result_get_iqn[0].split('=')[1].strip()
                            config_server['igroup'] = InitiatorGroup(igroup_spec)
                    host.close()

                config_server['volclone_topology'] = list()
                config_server['lun_mapping'] = list()
                for bkp_cs in bkp2clone['mongo_topology']['config_servers']:
                    if bkp_cs['stateStr'] == spec_cs_member['clone_from'].upper():
                        config_server['storage_info'] = dict()
                        config_server['storage_info']['lvm_vgname'] = bkp_cs['storage_info']['lvm_vgname']
                        config_server['storage_info']['fs_type'] = bkp_cs['storage_info']['fs_type']
                        config_server['storage_info']['mdb_device'] = bkp_cs['storage_info']['mdb_device']
                        for vol in bkp_cs['storage_info']['volume_topology']:
                            clone_spec = dict()
                            clone_spec['volume'] = self.clone_name + '_' + vol['volume'] + '_' + str(int(self.clone_uid))
                            clone_spec['parent-volume'] = vol['volume']
                            clone_spec['parent-snapshot'] = self.backup_name
                            flexclone = FlexClone(clone_spec)
                            config_server['volclone_topology'].append(flexclone)
                            logging.info('Volume {} ready to be cloned as {} on host {}'.format(vol['volume'],
                                                                                                clone_spec['volume'],
                                                                                                spec_cs_member['hostname']
                                                                                                ))
                            lun_spec = dict()
                            lun_spec['path'] = '/vol/' + clone_spec['volume'] + '/' + vol['lun-name']
                            lun_spec['igroup-name'] = igroup_spec['igroup-name']
                            lun_map = Lun(lun_spec)
                            config_server['lun_mapping'].append(lun_map)
                            logging.info('Preparing LUN {} to be mapped to igroup {}.'.format(lun_spec['path'],
                                                                                              lun_spec['igroup-name']
                                                                                              ))
                        break                
                
                cloned_cluster['config_servers']['members'].append(config_server)
            
            for spec_shard in self.clone_spec['shards']:
                shard_replset = dict()
                shard_replset['name'] = spec_shard['shard_name']
                shard_replset['members'] = list()
                shard_replset['reconfig'] = dict()
                
                # -- preparing shard replicaset reconfig document
                count = 0
                rc_sh_members = dict()
                for spec_sh_member in spec_shard['shard_members']:
                    rc_sh_members['members.' + str(count) + '._id'] = count
                    rc_sh_members['members.' + str(count) + '.host'] = spec_sh_member['hostname'] + ':' + \
                                                                       spec_sh_member['port']
                    rc_sh_members['members.' + str(count) + '.arbiterOnly'] = spec_sh_member['arbiter_only']
                    rc_sh_members['members.' + str(count) + '.buildIndexes'] = True
                    rc_sh_members['members.' + str(count) + '.hidden'] = False
                    rc_sh_members['members.' + str(count) + '.priority'] = 1
                    rc_sh_members['members.' + str(count) + '.tags'] = dict()
                    rc_sh_members['members.' + str(count) + '.slaveDelay'] = 0
                    rc_sh_members['members.' + str(count) + '.votes'] = 1
                    count += 1

                shard_replset['reconfig'] = rc_sh_members
                
                for spec_sh_member in spec_shard['shard_members']:
                    member = dict()
                    if spec_sh_member['arbiter_only']:
                        shard_replset['members'].append(spec_sh_member)
                        continue
                    
                    member = spec_sh_member
                    
                    host = HostConn(ipaddr=spec_sh_member['hostname'], username=self.username)
                    
                    igroup_spec = dict()
                    result_get_hostname = host.get_hostname()
                    if result_get_hostname[1] != 0:
                        logging.error('Could not get hostname from host {}.'.format(spec_sh_member['hostname']))
                        exit(1)
                    else:
                        logging.info('Preparing initiator group for host {}.'.format(spec_sh_member['hostname']))
                        igroup_spec['igroup-name'] = 'ig_' + result_get_hostname[0].strip('\n') + '_' + self.clone_name
                        igroup_spec['igroup-type'] = spec_sh_member['protocol']
                        igroup_spec['os-type'] = 'linux'
                        if spec_sh_member['protocol'] == 'iscsi':
                            result_get_iqn = host.get_iscsi_iqn()
                            if result_get_iqn[1] != 0:
                                logging.error('Could not get initiator name from host {}.'.format(spec_sh_member['hostname']))
                                exit(1)
                            else:
                                logging.info('Collecting initiator name on host {}.'.format(spec_sh_member['hostname']))
                                member['initiator'] = result_get_iqn[0].split('=')[1].strip()
                                member['igroup'] = InitiatorGroup(igroup_spec)
                        host.close()

                    member['volclone_topology'] = list()
                    member['lun_mapping'] = list()
                    for bkp_shard in bkp2clone['mongo_topology']['shards']:
                        for bkp_shard_member in bkp_shard['shard_members']:
                            if (bkp_shard_member['stateStr'] == spec_sh_member['clone_from'].upper()) and (
                                    bkp_shard['shard_name'] == spec_shard['shard_name']):
                                member['storage_info'] = dict()
                                member['storage_info']['lvm_vgname'] = bkp_shard_member['storage_info']['lvm_vgname']
                                member['storage_info']['fs_type'] = bkp_shard_member['storage_info']['fs_type']
                                member['storage_info']['mdb_device'] = bkp_shard_member['storage_info']['mdb_device']

                                for vol in bkp_shard_member['storage_info']['volume_topology']:
                                    if vol['svm-name'] != spec_sh_member['svm-name']:
                                        logging.error('You are asking a clone from a {} member on svm {}, but there is no {} on svm {} for shard {} on backup {}'.format(
                                                     spec_sh_member['clone_from'], spec_sh_member['svm-name'], spec_sh_member['clone_from'],
                                                     spec_sh_member['svm-name'], shard['shard_name'], self.backup_name
                                                     ))
                                        exit(1)

                                    clone_spec = dict()
                                    clone_spec['volume'] = self.clone_name + '_' + vol['volume'] + '_' + str(
                                        int(self.clone_uid))
                                    clone_spec['parent-volume'] = vol['volume']
                                    clone_spec['parent-snapshot'] = self.backup_name
                                    flexclone = FlexClone(clone_spec)
                                    member['volclone_topology'].append(flexclone)
                                    logging.info('Volume {} ready to be cloned as {} on host {}'.format(vol['volume'],
                                                                                                        clone_spec['volume'],
                                                                                                        spec_sh_member['hostname']
                                                                                                        ))
                                    lun_spec = dict()
                                    lun_spec['path'] = '/vol/' + clone_spec['volume'] + '/' + vol['lun-name']
                                    lun_spec['igroup-name'] = igroup_spec['igroup-name']
                                    lun_map = Lun(lun_spec)
                                    member['lun_mapping'].append(lun_map)
                                    logging.info('Preparing LUN {} to be mapped to igroup {}.'.format(lun_spec['path'],
                                                                                                      lun_spec['igroup-name']
                                                                                                      ))
                                shard_replset['members'].append(member)
                                break

                cloned_cluster['shards'].append(shard_replset)

            # -- Stage 2 :: Executing it
            # -- running steps to clone config servers
            for cs in cloned_cluster['config_servers']['members']:
                # -- Preparing recover and normal mode start string
                if self.clone_spec['defaults']['dir_per_db']:
                    recover_mode = '/usr/bin/mongod --logpath ' + self.clone_spec['defaults']['log_path'] + \
                                   ' --dbpath ' + cs['mountpoint'] + ' --bind_ip ' + cs['hostname'] + ' --port ' + \
                                   cs['port'] + ' --fork --directoryperdb'
                    normal_mode = '/usr/bin/mongod --logpath ' + self.clone_spec['defaults']['log_path'] + \
                                  ' --dbpath ' + cs['mountpoint'] + ' --bind_ip ' + cs['hostname'] + ' --port ' + \
                                  cs['port'] + ' --replSet ' + cloned_cluster['config_servers']['setname'] + \
                                  ' --fork --directoryperdb --configsvr'
                else:
                    recover_mode = '/usr/bin/mongod --logpath ' + self.clone_spec['defaults']['log_path'] + \
                                   ' --dbpath ' + cs['mountpoint'] + ' --bind_ip ' + cs['hostname'] + ' --port ' + \
                                   cs['port'] + ' --fork'
                    normal_mode = '/usr/bin/mongod --logpath ' + self.clone_spec['defaults']['log_path'] + \
                                  ' --dbpath ' + cs['mountpoint'] + ' --bind_ip ' + cs['hostname'] + ' --port ' + \
                                  cs['port'] + ' --replSet ' + cloned_cluster['config_servers']['setname'] + \
                                  ' --fork --configsvr'

                # -- openning a ssh connection to run host side commands
                host = HostConn(ipaddr=cs['hostname'], username=self.username)

                # -- if member is only an arbiter, there isn't any netapp action to be taken.
                if cs['arbiter_only']:
                    # -- removing mongod.lock and mongod.pid
                    host.remove_file(cs['mountpoint'] + '/mongod.lock')
                    host.remove_file('/var/run/mongodb/mongod.pid')

                    result = host.run_command('/sbin/runuser -l mongod -g mongod -c "' + recover_mode + '"')
                    if result[1] != 0:
                        logging.error('Cannot start mongodb in recover mode on host {}.'.format(cs['hostname']))
                        exit(1)
                    else:
                        logging.info('MongoDB has been started in recover mode on host {}.'.format(cs['hostname']))

                    # -- Updating ReplicaSet info
                    mdb_uri = 'mongodb://' + cs['hostname'] + ':' + cs['port']
                    mdb_session = MongoDBCluster(mongodb_uri=mdb_uri)
                    mdb_session.update_doc(dbname='local', collection='system.replset',
                                           update_filter={'_id': cloned_cluster['config_servers']['setname']},
                                           update_doc={'$unset': {'members': ''}}
                                           )
                    mdb_session.update_doc(dbname='local', collection='system.replset',
                                           update_filter={'_id': cloned_cluster['config_servers']['setname']},
                                           update_doc={'$set': {'members': []}}
                                           )
                    mdb_session.update_doc(dbname='local', collection='system.replset',
                                           update_filter={'_id': cloned_cluster['config_servers']['setname']},
                                           update_doc={'$set': cloned_cluster['config_servers']['reconfig']}
                                           )

                    # -- Stopping MongoDB recover mode
                    result = host.run_command('pkill mongod')
                    if result[1] != 0:
                        logging.error('Cannot kill mongoDB on host {}'.format(cs['hostname']))
                        exit(1)
                    else:
                        logging.info('MongoDB has been stopped on host {}.'.format(cs['hostname']))
                        host.remove_file(cs['mountpoint'] + '/mongod.lock')
                        host.remove_file('/var/run/mongodb/mongod.pid')

                    sleep(5)

                    # -- Starting MongoDB normal mode
                    result = host.run_command('/sbin/runuser -l mongod -g mongod -c "' + normal_mode + '"')
                    if result[1] != 0:
                        logging.error('Cannot start mongodb in normal mode on host {}.'.format(cs['hostname']))
                        exit(1)
                    else:
                        logging.info('MongoDB has been started in normal mode on host {}.'.format(cs['hostname']))
                        host.close()

                    continue

                kdb_ntapsys = kdb_session['ntapsystems']
                ntapsys = kdb_ntapsys.find_one({'svm-name': cs['svm-name']})
                if ntapsys is None:
                    logging.error('Cannot find SVM {} in the netapp repository collection.'.format(cs['svm-name']))
                    exit(1)

                svm_session = ClusterSession(cluster_ip=ntapsys['netapp-ip'], user=ntapsys['username'],
                                             password=ntapsys['password'], vserver=ntapsys['svm-name'])

                result = cs['igroup'].create(svm=svm_session)
                if result[0] == 'failed':
                    logging.error('Failed to create initiator group {} for host {}.'.format(cs['igroup'].initiator_group_name,
                                                                                            cs['hostname']))
                    exit(1)
                else:
                    logging.info('Initiator group {} has been created for host {}.'.format(cs['igroup'].initiator_group_name,
                                                                                           cs['hostname']))
                    result = cs['igroup'].add_initiators(svm=svm_session, initiator_list=cs['initiator'])
                    if result[0] == 'failed':
                        logging.error('Failed to add initiator {} to igroup {} for host {}.'.format(cs['initiator'],
                                                                                                    cs['igroup'].initiator_group_name,
                                                                                                    cs['hostname']))
                        exit(1)
                    else:
                        logging.info('Initiator {} has been added to {} for host {}.'.format(cs['initiator'],
                                                                                             cs['igroup'].initiator_group_name,
                                                                                             cs['hostname']))
                for volclone in cs['volclone_topology']:
                    result = volclone.create(svm=svm_session)
                    if result[0] == 'failed':
                        logging.error('Failed to create flexclone {} for host {}.'.format(volclone.volume,
                                                                                          cs['hostname']))
                        exit(1)
                    else:
                        logging.info('FlexClone {} has been created.'.format(volclone.volume))

                for lun in cs['lun_mapping']:
                    result = lun.mapping(svm=svm_session)
                    if result[0] == 'failed':
                        logging.error('Failed to map LUN {} to igroup {} for host {}.'.format(lun.path,
                                                                                              lun.igroup_name,
                                                                                              cs['hostname']))
                        exit(1)
                    else:
                        logging.info('LUN {} has been mapped to igroup {} for host {}.'.format(lun.path,
                                                                                               lun.igroup_name,
                                                                                               cs['hostname']))

                result = host.iscsi_send_targets(iscsi_target=cs['iscsi_target'])
                if result[1] != 0:
                    logging.error('{} on host {}'.format(result[0], cs['hostname']))
                    exit(1)
                else:
                    logging.info('Discovering targets on {} for host {}.'.format(cs['iscsi_target'], cs['hostname']))

                result = host.iscsi_node_login()
                if result[1] != 0:
                    logging.error('{} on host {}.'.format(result[0], cs['hostname']))
                    exit(1)
                else:
                    logging.info('Logged in to {} targets and ready to rescan devices on host {}.'.format(cs['igroup'].initiator_group_type,
                                                                                                          cs['hostname']))

                result = host.iscsi_rescan()
                if result[1] != 0:
                    logging.error('Could not rescan {} devices on host {}.'.format(cs['igroup'].initiator_group_type,
                                                                                   cs['hostname']))
                    exit(1)
                else:
                    logging.info('{} devices have been scanned on host {}.'.format(cs['igroup'].initiator_group_type,
                                                                                   cs['hostname']))

                result = host.enable_vg(vg_name=cs['storage_info']['lvm_vgname'])
                if result[1] != 0:
                    logging.error('Could not enable volume group {} on host {}.'.format(cs['storage_info']['lvm_vgname'],
                                                                                        cs['hostname']))
                    exit(1)
                else:
                    logging.info('Volume Group {} has been activated on host {}.'.format(cs['storage_info']['lvm_vgname'],
                                                                                         cs['hostname']))

                result = host.mount_fs(fs_mountpoint=cs['mountpoint'], fs_type=cs['storage_info']['fs_type'],
                                       device=cs['storage_info']['mdb_device'])
                if result[1] != 0:
                    logging.error('Could not mount device {} on host {}.'.format(cs['storage_info']['mdb_device'],
                                                                                 cs['hostname']))
                    exit(1)
                else:
                    logging.info('Device {} has been mounted to {} on host {}.'.format(cs['storage_info']['mdb_device'],
                                                                                       cs['mountpoint'],
                                                                                       cs['hostname']))

                # -- Starting MongoDB on recover mode
                result = host.run_command('/sbin/runuser -l mongod -g mongod -c "' + recover_mode + '"')
                if result[1] != 0:
                    logging.error('Cannot start mongodb in recover mode on host {}.'.format(cs['hostname']))
                    exit(1)
                else:
                    logging.info('MongoDB has been started in recover mode on host {}.'.format(cs['hostname']))

                # -- Updating ReplicaSet info
                mdb_uri = 'mongodb://' + cs['hostname'] + ':' + cs['port']
                mdb_session = MongoDBCluster(mongodb_uri=mdb_uri)
                mdb_session.update_doc(dbname='local', collection='system.replset',
                                       update_filter={'_id': cloned_cluster['config_servers']['setname']},
                                       update_doc={'$unset': {'members': ''}}
                                       )
                mdb_session.update_doc(dbname='local', collection='system.replset',
                                       update_filter={'_id': cloned_cluster['config_servers']['setname']},
                                       update_doc={'$set': {'members': []}}
                                       )
                mdb_session.update_doc(dbname='local', collection='system.replset',
                                       update_filter={'_id': cloned_cluster['config_servers']['setname']},
                                       update_doc={'$set': cloned_cluster['config_servers']['reconfig']}
                                       )

                mdb_session.delete_doc(dbname='admin', collection='system.version',
                                       delete_filter={'_id': 'minOpTimeRecovery'})

                for spec_shard in self.clone_spec['shards']:
                    shard_string = spec_shard['shard_name'] + '/'
                    count = 1
                    for spec_sh_member in spec_shard['shard_members']:
                        if count < len(spec_shard['shard_members']):
                            shard_string += spec_sh_member['hostname'] + ':' + spec_sh_member['port'] + ','
                        elif count == len(spec_shard['shard_members']):
                            shard_string += spec_sh_member['hostname'] + ':' + spec_sh_member['port']
                        count += 1

                    mdb_session.update_doc(dbname='config', collection='shards', 
                                           update_filter={'_id': spec_shard['shard_name']},
                                           update_doc={'$set': {'host': shard_string}})

                # -- Stopping MongoDB recover mode
                result = host.run_command('pkill mongod')
                if result[1] != 0:
                    logging.error('Cannot kill mongoDB on host {}'.format(cs['hostname']))
                    exit(1)
                else:
                    logging.info('MongoDB has been stopped on host {}.'.format(cs['hostname']))
                    host.remove_file(cs['mountpoint'] + '/mongod.lock')
                    host.remove_file('/var/run/mongodb/mongod.pid')

                sleep(5)

                # -- Starting MongoDB normal mode
                result = host.run_command('/sbin/runuser -l mongod -g mongod -c "' + normal_mode + '"')
                if result[1] != 0:
                    logging.error('Cannot start mongodb in normal mode on host {}.'.format(cs['hostname']))
                    exit(1)
                else:
                    logging.info('MongoDB has been started in normal mode on host {}.'.format(cs['hostname']))
                    host.close()

            # -- running steps to clone shards
            for shard in cloned_cluster['shards']:
                for shard_member in shard['members']:
                    # -- Preparing recover and normal mode start string
                    if self.clone_spec['defaults']['dir_per_db']:
                        recover_mode = '/usr/bin/mongod --logpath ' + self.clone_spec['defaults']['log_path'] + \
                                       ' --dbpath ' + shard_member['mountpoint'] + ' --bind_ip ' + shard_member['hostname'] + ' --port ' + \
                                       shard_member['port'] + ' --fork --directoryperdb'
                        normal_mode = '/usr/bin/mongod --logpath ' + self.clone_spec['defaults']['log_path'] + \
                                      ' --dbpath ' + shard_member['mountpoint'] + ' --bind_ip ' + shard_member['hostname'] + ' --port ' + \
                                      shard_member['port'] + ' --replSet ' + shard['name'] + \
                                      ' --fork --directoryperdb --shardsvr'
                    else:
                        recover_mode = '/usr/bin/mongod --logpath ' + self.clone_spec['defaults']['log_path'] + \
                                       ' --dbpath ' + shard_member['mountpoint'] + ' --bind_ip ' + shard_member['hostname'] + ' --port ' + \
                                       shard_member['port'] + ' --fork'
                        normal_mode = '/usr/bin/mongod --logpath ' + self.clone_spec['defaults']['log_path'] + \
                                      ' --dbpath ' + shard_member['mountpoint'] + ' --bind_ip ' + shard_member['hostname'] + ' --port ' + \
                                      shard_member['port'] + ' --replSet ' + shard['name'] + ' --fork --shardsvr'

                    # -- openning a ssh connection to run host side commands
                    host = HostConn(ipaddr=shard_member['hostname'], username=self.username)

                    # -- if member is only an arbiter, there isn't any netapp action to be taken.
                    if shard_member['arbiter_only']:
                        # -- removing mongod.lock and mongod.pid
                        host.remove_file(shard_member['mountpoint'] + '/mongod.lock')
                        host.remove_file('/var/run/mongodb/mongod.pid')

                        result = host.run_command('/sbin/runuser -l mongod -g mongod -c "' + recover_mode + '"')
                        if result[1] != 0:
                            logging.error('Cannot start mongodb in recover mode on host {}.'.format(shard_member['hostname']))
                            exit(1)
                        else:
                            logging.info('MongoDB has been started in recover mode on host {}.'.format(shard_member['hostname']))

                        # -- Updating ReplicaSet info
                        mdb_uri = 'mongodb://' + shard_member['hostname'] + ':' + shard_member['port']
                        mdb_session = MongoDBCluster(mongodb_uri=mdb_uri)
                        mdb_session.update_doc(dbname='local', collection='system.replset',
                                               update_filter={'_id': shard['name']},
                                               update_doc={'$unset': {'members': ''}}
                                               )
                        mdb_session.update_doc(dbname='local', collection='system.replset',
                                               update_filter={'_id': shard['name']},
                                               update_doc={'$set': { 'members': []}}
                                               )
                        mdb_session.update_doc(dbname='local', collection='system.replset',
                                               update_filter={'_id': shard['name']},
                                               update_doc={'$set': shard['reconfig']}
                                               )

                        # -- Stopping MongoDB recover mode
                        result = host.run_command('pkill mongod')
                        if result[1] != 0:
                            logging.error('Cannot kill mongoDB on host {}'.format(shard_member['hostname']))
                            exit(1)
                        else:
                            logging.info('MongoDB has been stopped on host {}.'.format(shard_member['hostname']))
                            host.remove_file(shard_member['mountpoint'] + '/mongod.lock')
                            host.remove_file('/var/run/mongodb/mongod.pid')

                        sleep(5)

                        # -- Starting MongoDB normal mode
                        result = host.run_command('/sbin/runuser -l mongod -g mongod -c "' + normal_mode + '"')
                        if result[1] != 0:
                            logging.error('Cannot start mongodb in normal mode on host {}.'.format(shard_member['hostname']))
                            exit(1)
                        else:
                            logging.info('MongoDB has been started in normal mode on host {}.'.format(shard_member['hostname']))
                            host.close()

                        continue

                    kdb_ntapsys = kdb_session['ntapsystems']
                    ntapsys = kdb_ntapsys.find_one({'svm-name': shard_member['svm-name']})
                    if ntapsys is None:
                        logging.error('Cannot find SVM {} in the netapp repository collection.'.format(shard_member['svm-name']))
                        exit(1)

                    svm_session = ClusterSession(cluster_ip=ntapsys['netapp-ip'], user=ntapsys['username'],
                                                 password=ntapsys['password'], vserver=ntapsys['svm-name'])

                    result = shard_member['igroup'].create(svm=svm_session)
                    if result[0] == 'failed':
                        logging.error(
                            'Failed to create initiator group {} for host {}.'.format(shard_member['igroup'].initiator_group_name,
                                                                                      shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info(
                            'Initiator group {} has been created for host {}.'.format(shard_member['igroup'].initiator_group_name,
                                                                                      shard_member['hostname']))
                        result = shard_member['igroup'].add_initiators(svm=svm_session, initiator_list=shard_member['initiator'])
                        if result[0] == 'failed':
                            logging.error('Failed to add initiator {} to igroup {} for host {}.'.format(shard_member['initiator'],
                                                                                                        shard_member[
                                                                                                            'igroup'].initiator_group_name,
                                                                                                        shard_member['hostname']))
                            exit(1)
                        else:
                            logging.info('Initiator {} has been added to {} for host {}.'.format(shard_member['initiator'],
                                                                                                 shard_member[
                                                                                                     'igroup'].initiator_group_name,
                                                                                                 shard_member['hostname']))
                    for volclone in shard_member['volclone_topology']:
                        result = volclone.create(svm=svm_session)
                        if result[0] == 'failed':
                            logging.error('Failed to create flexclone {} for host {}.'.format(volclone.volume,
                                                                                              shard_member['hostname']))
                            exit(1)
                        else:
                            logging.info('FlexClone {} has been created.'.format(volclone.volume))

                    for lun in shard_member['lun_mapping']:
                        result = lun.mapping(svm=svm_session)
                        if result[0] == 'failed':
                            logging.error('Failed to map LUN {} to igroup {} for host {}.'.format(lun.path,
                                                                                                  lun.igroup_name,
                                                                                                  shard_member['hostname']))
                            exit(1)
                        else:
                            logging.info('LUN {} has been mapped to igroup {} for host {}.'.format(lun.path,
                                                                                                   lun.igroup_name,
                                                                                                   shard_member['hostname']))

                    result = host.iscsi_send_targets(iscsi_target=shard_member['iscsi_target'])
                    if result[1] != 0:
                        logging.error('{} on host {}'.format(result[0], shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info(
                            'Discovering targets on {} for host {}.'.format(shard_member['iscsi_target'], shard_member['hostname']))

                    result = host.iscsi_node_login()
                    if result[1] != 0:
                        logging.error('{} on host {}.'.format(result[0], shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info('Logged in to {} targets and ready to rescan devices on host {}.'.format(
                            shard_member['igroup'].initiator_group_type,
                            shard_member['hostname']))

                    result = host.iscsi_rescan()
                    if result[1] != 0:
                        logging.error(
                            'Could not rescan {} devices on host {}.'.format(shard_member['igroup'].initiator_group_type,
                                                                             shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info(
                            '{} devices have been scanned on host {}.'.format(shard_member['igroup'].initiator_group_type,
                                                                              shard_member['hostname']))

                    result = host.enable_vg(vg_name=shard_member['storage_info']['lvm_vgname'])
                    if result[1] != 0:
                        logging.error(
                            'Could not enable volume group {} on host {}.'.format(shard_member['storage_info']['lvm_vgname'],
                                                                                  shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info(
                            'Volume Group {} has been activated on host {}.'.format(shard_member['storage_info']['lvm_vgname'],
                                                                                    shard_member['hostname']))

                    result = host.mount_fs(fs_mountpoint=shard_member['mountpoint'], fs_type=shard_member['storage_info']['fs_type'],
                                           device=shard_member['storage_info']['mdb_device'])
                    if result[1] != 0:
                        logging.error('Could not mount device {} on host {}.'.format(shard_member['storage_info']['mdb_device'],
                                                                                     shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info(
                            'Device {} has been mounted to {} on host {}.'.format(shard_member['storage_info']['mdb_device'],
                                                                                  shard_member['mountpoint'],
                                                                                  shard_member['hostname']))

                    # -- Starting MongoDB on recover mode
                    result = host.run_command('/sbin/runuser -l mongod -g mongod -c "' + recover_mode + '"')
                    if result[1] != 0:
                        logging.error('Cannot start mongodb in recover mode on host {}.'.format(shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info('MongoDB has been started in recover mode on host {}.'.format(shard_member['hostname']))

                    # -- Updating ReplicaSet info
                    mdb_uri = 'mongodb://' + shard_member['hostname'] + ':' + shard_member['port']
                    mdb_session = MongoDBCluster(mongodb_uri=mdb_uri)
                    mdb_session.update_doc(dbname='local', collection='system.replset',
                                           update_filter={'_id': shard['name']},
                                           update_doc={'$unset': {'members': ''}}
                                           )
                    mdb_session.update_doc(dbname='local', collection='system.replset',
                                           update_filter={'_id': shard['name']},
                                           update_doc={'$set': {'members': []}}
                                           )
                    mdb_session.update_doc(dbname='local', collection='system.replset',
                                           update_filter={'_id': shard['name']},
                                           update_doc={'$set': shard['reconfig']}
                                           )

                    mdb_session.delete_doc(dbname='admin', collection='system.version',
                                           delete_filter={'_id': 'minOpTimeRecovery'})

                    # -- creating configsrvConnectionString
                    conn_string = self.clone_spec['config_servers']['setname'] + '/'
                    for spec_cs_member in self.clone_spec['config_servers']['members']:
                        hostport = spec_cs_member['hostname'] + ':' + spec_cs_member['port']
                        conn_string += hostport + ','

                    mdb_session.update_doc(dbname='admin', collection='system.version',
                                           update_filter={'_id': 'shardIdentity'},
                                           update_doc={'$set': {'configsvrConnectionString': conn_string[:-1]}})

                    # -- Stopping MongoDB recover mode
                    result = host.run_command('pkill mongod')
                    if result[1] != 0:
                        logging.error('Cannot kill mongoDB on host {}'.format(shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info('MongoDB has been stopped on host {}.'.format(shard_member['hostname']))
                        host.remove_file(shard_member['mountpoint'] + '/mongod.lock')
                        host.remove_file('/var/run/mongodb/mongod.pid')

                    sleep(5)

                    # -- Starting MongoDB normal mode
                    result = host.run_command('/sbin/runuser -l mongod -g mongod -c "' + normal_mode + '"')
                    if result[1] != 0:
                        logging.error('Cannot start mongodb in normal mode on host {}.'.format(shard_member['hostname']))
                        exit(1)
                    else:
                        logging.info('MongoDB has been started in normal mode on host {}.'.format(shard_member['hostname']))
                        host.close()

            # -- Starting mongoses
            count = 1
            configdb = self.clone_spec['config_servers']['setname'] + '/'
            for spec_cs_member in self.clone_spec['config_servers']['members']:
                if count < len(self.clone_spec['config_servers']['members']):
                    configdb += spec_cs_member['hostname'] + ':' + spec_cs_member['port'] + ','
                elif count == len(self.clone_spec['config_servers']['members']):
                    configdb += spec_cs_member['hostname'] + ':' + spec_cs_member['port']
                count += 1

            for mongos in self.clone_spec['mongos']:
                host = HostConn(ipaddr=mongos, username=self.username)
                result = host.run_command('/usr/bin/mongos --bind_ip ' + mongos + ' --configdb ' + configdb +
                                          ' --fork --logpath /var/log/mongodb/mongos.log')
                if result[1] != 0:
                    logging.error('Could not start mongos on host {}.'.format(mongos))
                    exit(1)
                else:
                    logging.info('mongos has been started on host {}.'.format(mongos))
                    host.close()

            # -- Stage 3 :: Cataloging it
            clone_metadata = dict()
            clone_metadata['clone_name'] = self.clone_name
            clone_metadata['backup_name'] = self.backup_name
            clone_metadata['cluster_name'] = self.cluster_name
            clone_metadata['clone_uid'] = int(self.clone_uid)
            clone_metadata['created_at'] = datetime.now()
            clone_metadata['desc'] = self.desc
            clone_metadata['mongos'] = self.clone_spec['mongos']
            clone_metadata['config_server'] = list()
            clone_metadata['shards'] = list()
            for cs_member in cloned_cluster['config_servers']['members']:
                member = dict()
                if cs_member['arbiter_only']:
                    member['hostname'] = cs_member['hostname']
                    member['arbiter_only'] = cs_member['arbiter_only']
                    continue
                    
                member['hostname'] = cs_member['hostname']
                member['arbiter_only'] = cs_member['arbiter_only']
                member['igroup_name'] = cs_member['igroup'].initiator_group_name
                member['svm_name'] = cs_member['svm-name']
                member['mountpoint'] = cs_member['mountpoint']
                member['lvm_vgname'] = cs_member['storage_info']['lvm_vgname']
                member['volclone_topology'] = list()
                for vol in cs_member['volclone_topology']:
                    member['volclone_topology'].append(vol.volume)

                clone_metadata['config_server'].append(member)

            for shard in cloned_cluster['shards']:
                sh = dict()
                sh['name'] = shard['name']
                sh['members'] = list()
                for sh_member in shard['members']:
                    member = dict()
                    if sh_member['arbiter_only']:
                        member['hostname'] = sh_member['hostname']
                        member['arbiter_only'] = sh_member['arbiter_only']
                        continue
                        
                    member['hostname'] = sh_member['hostname']
                    member['arbiter_only'] = sh_member['arbiter_only']
                    member['igroup_name'] = sh_member['igroup'].initiator_group_name
                    member['svm_name'] = sh_member['svm-name']
                    member['mountpoint'] = sh_member['mountpoint']
                    member['lvm_vgname'] = sh_member['storage_info']['lvm_vgname']
                    member['volclone_topology'] = list()
                    for vol in sh_member['volclone_topology']:
                        member['volclone_topology'].append(vol.volume)

                    sh['members'].append(member)
                clone_metadata['shards'].append(sh)

            kdb_clones = kdb_session['clones']
            result = kdb_clones.insert_one(clone_metadata).inserted_id
            if result is None:
                logging.error('Clone has been created but it was not inserted into the catalog.')
                exit(1)
            else:
                logging.info('Clone has been created successfully.')

    def delete(self, kdb_session=None):
        kdb_clones = kdb_session['clones']
        clone2del = kdb_clones.find_one({'clone_name': self.clone_name})

        if clone2del is None:
            logging.error('Cannot find clone {}.'.format(self.clone_name))
            exit(1)

        for mongos in clone2del['mongos']:
            host = HostConn(ipaddr=mongos, username=self.username)
            result = host.run_command('pkill mongos')
            if result[1] != 0:
                logging.error('Could not kill mongos on host {}.'.format(mongos))
#                exit(1)
            else:
                logging.info('mongos has been stopped on host {}.'.format(mongos))
                host.close()

        for cs_member in clone2del['config_server']:
            host = HostConn(ipaddr=cs_member['hostname'], username=self.username)
            
            if cs_member['arbiter_only']:
                result = host.run_command('pkill mongod')
                if result[1] != 0:
                    logging.error('Could not kill mongod on host {}'.format(cs_member['hostname']))
#                    exit(1)
                else:
                    logging.info('mongod has been stopped on host {}.'.format(cs_member['hostname']))
                    continue

            result = host.run_command('pkill mongod')
            if result[1] != 0:
                logging.error('Could not kill mongod on host {}'.format(cs_member['hostname']))
#                exit(1)
            else:
                logging.info('mongod has been stopped on host {}.'.format(cs_member['hostname']))
                sleep(3)

            result = host.umount_fs(fs_mountpoint=cs_member['mountpoint'])
            if result[1] != 0:
                logging.error('Could not unmount mongoDB file system {} on host {}.'.format(cs_member['mountpoint'],
                                                                                           cs_member['hostname']))
#                exit(1)
            else:
                logging.info('mongoDB file system {} has been unmounted on host {}.'.format(cs_member['mountpoint'],
                                                                                            cs_member['hostname']))
                sleep(3)

            result = host.disable_vg(vg_name=cs_member['lvm_vgname'])
            if result[1] != 0:
                logging.error('Could not disable volume group {} on host {}.'.format(cs_member['lvm_vgname'],
                                                                                     cs_member['hostname']))
#                exit(1)
            else:
                logging.info('Volume Group {} has been disabled on host {}.'.format(cs_member['lvm_vgname'],
                                                                                    cs_member['hostname']))

            # -- establishing NetApp cluster session to delete flexclones
            # -- Putting volumes offline, then delete them.
            # -- Destroying igroups
            kdb_ntapsys = kdb_session['ntapsystems']
            ntapsys = kdb_ntapsys.find_one({'svm-name': cs_member['svm_name']})
            svm_session = ClusterSession(cluster_ip=ntapsys['netapp-ip'], user=ntapsys['username'],
                                         password=ntapsys['password'], vserver=ntapsys['svm-name'])

            for vol in cs_member['volclone_topology']:
                vol_spec = dict()
                vol_spec['volume'] =  vol
                volclone = Volume(vol_spec)
                result = volclone.destroy(svm=svm_session)
                if result[0] == 'failed':
                    logging.error('Could not delete flexvolume {} on SVM {}.'.format(vol, cs_member['svm_name']))
#                    exit(1)
                else:
                    logging.info('FlexClone volume {} has been deleted on SVM {}.'.format(vol, cs_member['svm_name']))

            igroup_spec = dict()
            igroup_spec['igroup-name'] = cs_member['igroup_name']
            igroup = InitiatorGroup(igroup_spec)

            result = igroup.destroy(svm=svm_session)
            if result[0] == 'failed':
                logging.error('Could not destroy igroup {} on SVM {}.'.format(cs_member['igroup_name'],
                                                                              cs_member['svm_name']))
#                exit(1)
            else:
                logging.info('Igroup {} has been destroyed on SVM {}.'.format(cs_member['igroup_name'],
                                                                              cs_member['svm_name']))

            result = host.iscsi_rescan()
            if result[1] != 0:
                logging.error('Could not rescan devices on host {}.'.format(cs_member['hostname']))
#                exit(1)
            else:
                logging.info('Stale devices has been removed on host {}.'.format(cs_member['hostname']))
                host.close()

        for shard in clone2del['shards']:
            for sh_member in shard['members']:
                host = HostConn(ipaddr=sh_member['hostname'], username=self.username)

                if sh_member['arbiter_only']:
                    result = host.run_command('pkill mongod')
                    if result[1] != 0:
                        logging.error('Could not kill mongod on host {}'.format(sh_member['hostname']))
#                        exit(1)
                    else:
                        logging.info('mongod has been stopped on host {}.'.format(sh_member['hostname']))
                        continue

                result = host.run_command('pkill mongod')
                if result[1] != 0:
                    logging.error('Could not kill mongod on host {}'.format(sh_member['hostname']))
#                    exit(1)
                else:
                    logging.info('mongod has been stopped on host {}.'.format(sh_member['hostname']))
                    sleep(3)

                result = host.umount_fs(fs_mountpoint=sh_member['mountpoint'])
                if result[1] != 0:
                    logging.error('Could not unmount mongoDB file system {} on host {}.'.format(sh_member['mountpoint'],
                                                                                                sh_member['hostname']))
#                    exit(1)
                else:
                    logging.info('mongoDB file system {} has been unmounted on host {}.'.format(sh_member['mountpoint'],
                                                                                                sh_member['hostname']))
                    sleep(3)

                result = host.disable_vg(vg_name=sh_member['lvm_vgname'])
                if result[1] != 0:
                    logging.error('Could not disable volume group {} on host {}.'.format(sh_member['lvm_vgname'],
                                                                                         sh_member['hostname']))
#                    exit(1)
                else:
                    logging.info('Volume Group {} has been disabled on host {}.'.format(sh_member['lvm_vgname'],
                                                                                        sh_member['hostname']))

                # -- establishing NetApp cluster session to delete flexclones
                # -- Putting volumes offline, then delete them.
                # -- Destroying igroups
                kdb_ntapsys = kdb_session['ntapsystems']
                ntapsys = kdb_ntapsys.find_one({'svm-name': sh_member['svm_name']})
                svm_session = ClusterSession(cluster_ip=ntapsys['netapp-ip'], user=ntapsys['username'],
                                             password=ntapsys['password'], vserver=ntapsys['svm-name'])

                for vol in sh_member['volclone_topology']:
                    vol_spec = dict()
                    vol_spec['volume'] = vol
                    volclone = Volume(vol_spec)
                    result = volclone.destroy(svm=svm_session)
                    if result[0] == 'failed':
                        logging.error('Could not delete flexvolume {} on SVM {}.'.format(vol, sh_member['svm_name']))
#                        exit(1)
                    else:
                        logging.info(
                            'FlexClone volume {} has been deleted on SVM {}.'.format(vol, sh_member['svm_name']))

                igroup_spec = dict()
                igroup_spec['igroup-name'] = sh_member['igroup_name']
                igroup = InitiatorGroup(igroup_spec)

                result = igroup.destroy(svm=svm_session)
                if result[0] == 'failed':
                    logging.error('Could not destroy igroup {} on SVM {}.'.format(sh_member['igroup_name'],
                                                                                  sh_member['svm_name']))
#                    exit(1)
                else:
                    logging.info('Igroup {} has been destroyed on SVM {}.'.format(sh_member['igroup_name'],
                                                                                  sh_member['svm_name']))

                result = host.iscsi_rescan()
                if result[1] != 0:
                    logging.error('Could not rescan devices on host {}.'.format(sh_member['hostname']))
#                    exit(1)
                else:
                    logging.info('Stale devices has been removed on host {}.'.format(sh_member['hostname']))
                    host.close()

        result = kdb_clones.delete_one({'clone_name': self.clone_name})
        if result is not None:
            logging.info('Clone {} has been deleted.'.format(self.clone_name))

    def list(self, kdb_session=None):
        kdb_clones = kdb_session['clones']
        result = kdb_clones.find({'cluster_name': self.cluster_name})
        if result is None:
            print 'There are no clones to be listed for {} cluster.'.format(self.cluster_name)
        else:
            for clone in result:
                print 'Clone Name..: {}'.format(clone['clone_name'])
                print 'Created at..: {}'.format(clone['created_at'].strftime('%Y-%m-%d %H:%M:%S.%f'))
                print 'Based on....: {}'.format(clone['backup_name'])
                print 'Description.: {}'.format(clone['desc'])
                print ''

        # print '{:30} \t {:30} \t {:30} \t {:40}'.format('Clone Name', 'Created at', 'Based on', 'Description')
        # for clone in result:
        #     print '{:30} \t {:30} \t {:30} \t {:40}'.format(clone['clone_name'], clone['created_at'].strftime('%c %Z'),
        #                                                     clone['backup_name'], clone['desc'])


class SubCmdRecover:
    def __init__(self, rec_spec=None):
        self.cluster_name = rec_spec['from-cluster-name']
        self.dest_mongodb_uri = rec_spec['dest-mongodb-uri']
        self.from_date = rec_spec['from-date']
        self.until_date = rec_spec['until-date']
        self.arch_repo_name = rec_spec['arch-repo-name']
        self.arch_repo_uri = rec_spec['arch-repo-uri']
        self.temp_coll = 'temp_' + self.cluster_name + '_' + str(int(time()))
        if 'skip_op_cfg' in rec_spec:
            self.skip_op = rec_spec['skip_op_cfg']

        if (mp.cpu_count()/2)-1 == 0:
            self.num_consumers = 1
        else:
            self.num_consumers = (mp.cpu_count()/2)-1

    def start(self):
        # Initiate a queue and lock to be used by consumers and producers
        recover_queue = multiprocessing.queues.JoinableQueue()

        # Recover Producer instance
        atd = ArchTempData(arch_repo_uri=self.arch_repo_uri, arch_repo_name=self.arch_repo_name,
                           source_cluster_name=self.cluster_name, begin_from=self.from_date, upto=self.until_date,
                           temp_coll=self.temp_coll, arch_queue=recover_queue)

        # Create a temporary collection containing the data that will be added back to the databases
        atd.create_temp_data()

        # Create a process to read temp data and insert in the queue
        atd.read_temp_data()

        # prepare the list of consumer instances
        consumers = list()
        num_procs = 0
        while num_procs < self.num_consumers:
            consumers.append(RecoverConsumer(arch_queue=recover_queue, dest_cluster_uri=self.dest_mongodb_uri))
            num_procs += 1

        # prepare list of consumer process instances
        consumer_procs = list()
        for consumer in consumers:
            consumer_procs.append(mp.Process(target=consumer.run))

        # kicking off consumer processes
        for consumer_proc in consumer_procs:
            consumer_proc.start()

        # Waiting processes to finish their work
        for consumer_proc in consumer_procs:
            consumer_proc.join()

        # Destroy temp data collection
        atd.destroy_temp_data()

        logging.info('Recover process has been completed.')


class SubCmdArchiver:
    def __init__(self, archiver_spec=None):
        self.arch_spec = archiver_spec

    def create(self, catalog_sess=None):
        catalog_sess.add(coll_name='archivers', doc=self.arch_spec)

    def delete(self, catalog_sess=None):
        catalog_sess.remove_one(coll_name='archivers', query={'cluster_name': self.arch_spec['cluster_name'],
                                                              'archiver_name': self.arch_spec['archiver_name']})
    @staticmethod
    def list(catalog_sess=None, cluster_name=None):
        archivers = catalog_sess.find_all(coll_name='archivers', query={'cluster_name': cluster_name})
        return archivers

    def stop(self, catalog_sess=None):
        pidfilename = catalog_sess.find_one(coll_name='archivers',
                                            query={'cluster_name': self.arch_spec['cluster_name'],
                                                   'archiver_name': self.arch_spec['archiver_name']})['pidfile']

        try:
            pidfile = open(pidfilename, 'r')
            ppid = pidfile.readline()
        except IOError, e:
            logging.error(e)

        Process(int(ppid)).terminate()
        catalog_sess.edit(coll_name='archivers', query={'cluster_name': self.arch_spec['cluster_name'],
                                                        'archiver_name': self.arch_spec['archiver_name']},
                          update={'$unset': {'pidfile': ''}})

    def start(self, catalog_sess=None):
        appKAPTR = AppKairosAPTR(cluster_name=self.arch_spec['cluster_name'],
                                 database_name=self.arch_spec['database_name'],
                                 collections=self.arch_spec['collections'], mongodb_uri=self.arch_spec['mongodb_uri'],
                                 archiver_name=self.arch_spec['archiver_name'],
                                 archive_repo_uri=self.arch_spec['archive_repo_uri'],
                                 archive_repo_name=self.arch_spec['archive_repo_name'])

        catalog_sess.edit(coll_name='archivers', query={'cluster_name': self.arch_spec['cluster_name'],
                                                        'archiver_name': self.arch_spec['archiver_name']},
                          update={'$set': { 'pidfile': appKAPTR.get_pidfilename()}})

        appKAPTR.start()

    def status(self, catalog_sess=None):
        # Getting the PID filename from Kairos' repository
        archiver = catalog_sess.find_one(coll_name='archivers',
                                         query={'cluster_name': self.arch_spec['cluster_name'],
                                                'archiver_name': self.arch_spec['archiver_name']})

        # Opening the PID file and getting the archiver PID
        if 'pidfile' not in archiver.keys():
            return False
        else:
            try:
                pidfile = open(archiver['pidfile'], 'r')
                ppid = pidfile.readline()
            except IOError, e:
                logging.error(e)
                return False

            # Instanciating a process to get info about it
            proc = Process(int(ppid))

            if proc.name() == 'python' and self.arch_spec['archiver_name'] in proc.cmdline():
                return True


class SubCmdOperations:
    def __init__(self, catalog_sess=None):
        self.catalog = catalog_sess

    def get_first_and_last_ops_per_coll(self, cluster_name=None):
        # Aggregation query to list the first and last operation grouped by database
        pipeline = [
            {'$sort': {'created_at': 1}},
            {'$group': {'_id': {'dbname':'$ns.db', 'collname':'$ns.coll'},
                        'firstOperation': {'$first': '$created_at'},
                        'lastOperation': {'$last': '$created_at'}
                        }
            },
            {'$sort': {'firstOperation': 1}}
        ]

        aggr_ops = self.catalog.run_aggregation(coll_name=cluster_name, aggr_pipeline=pipeline)
        return aggr_ops

    def get_first_and_last_ops_cluster(self, cluster_name=None):
        # Aggregation query to list the first and last operation for the whole cluster
        pipeline = [
            {'$sort': {'created_at': 1}},
            {'$group': {'_id': 'cluster',
                        'firstOperation': {'$first': '$created_at'},
                        'lastOperation': {'$last': '$created_at'}
                        }
            },
            {'$sort': {'firstOperation': 1}}
        ]

        aggr_ops = self.catalog.run_aggregation(coll_name=cluster_name, aggr_pipeline=pipeline)
        return aggr_ops

    def get_ops_per_type(self, cluster_name=None, from_date=None, until_date=None):
        # Aggregation query to list operations per type/database/coll
        pipeline = [
            {
                '$match': {
                    '$and': [
                        {'created_at': {'$gte': from_date}},
                        {'created_at': {'$lte': until_date}}
                    ]
                }
            },
            {
                '$group': {
                    '_id': {'dbname': '$ns.db', 'collname': '$ns.coll', 'op_type': '$operationType'},
                    'totalOps': {'$sum': 1}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'dbname': '$_id.dbname',
                    'collname': '$_id.collname',
                    'op_type': '$_id.op_type',
                    'totalOps': 1
                }
            },
            {
                '$group': {
                    '_id': {'dbname': '$dbname', 'collname': '$collname'},
                    'per_type': {
                        '$push': {'op_type': '$op_type', 'totalOps': '$totalOps'}
                    }
                }
            },
            {'$sort': {'_id': 1, 'per_type.op_type': 1}}
        ]

        aggr_ops = self.catalog.run_aggregation(coll_name=cluster_name, aggr_pipeline=pipeline)
        return aggr_ops

    def get_total_ops_per_collection(self, cluster_name=None, from_date=None, until_date=None):
        # Aggregation query to count all operations per database/coll
        pipeline = [
            {
                '$match': {
                    'created_at': {'$gte': from_date},
                    'created_at': {'$lte': until_date}
                }
            },
            {
                '$group': {
                    '_id': {'dbname': '$ns.db', 'collname': '$ns.coll', 'op_type': '$operationType'},
                    'totalOps': {'$sum': 1}
                }
            },
            {
                '$group': {
                    '_id': {'dbname':'$_id.dbname', 'collname': '$_id.collname'},
                    'totalCollOps': { '$sum': '$totalOps'}
                }
            }
        ]

        aggr_ops = self.catalog.run_aggregation(coll_name=cluster_name, aggr_pipeline=pipeline)
        return aggr_ops
