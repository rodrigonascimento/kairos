#!/usr/bin/env python

import logging
import xml.dom.minidom
from pymongo import MongoClient, errors
from sys import exit
from time import sleep, time
from datetime import datetime, timedelta
from ontap import ClusterSession, Snapshot
from mongodbcluster import MongoDBCluster
from host_conn import HostConn


class SubCmdMongodb:
    def __init__(self, mdbcluster_spec=None):
        if mdbcluster_spec is not None:
            for mdb_key in mdbcluster_spec.keys():
                if mdbcluster_spec[mdb_key] is None:
                    mdbcluster_spec.pop(mdb_key)

            self.mdb_spec = mdbcluster_spec

    def add(self, kdb_session, kdb_collection):
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

    def remove(self, kdb_session, kdb_collection):
        collection = kdb_session[kdb_collection]
        result = collection.delete_one({'cluster-name': self.mdb_spec['cluster-name']})

    def list(self, kdb_session, kdb_collection):
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

        # -- Getting MongoDB cluster topology
        topology = mdbcluster.get_topology()
        logging.info(self.backup['cluster-name'] + ' is a ' + topology['cluster_type'] + ' cluster.')

        if topology['cluster_type'] == 'replSet':
            for rs_member in topology['members']:
                if rs_member['stateStr'] == 'PRIMARY' or rs_member['stateStr'] == 'SECONDARY':
                    host = HostConn(ipaddr=rs_member['name'].split(':')[0], username=self.backup['username'])
                    rs_member['storage_info'] = host.get_storage_layout(cluster_info['mongodb-mongod-conf'])
                    host.close()
                    logging('Collecting info about host {}'.format(rs_member['name'].split(':')[0]))

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
        bkp_metadata = dict()
        bkp_metadata['backup_name'] = self.backup['backup-name']
        bkp_metadata['cluster_name'] = self.backup['cluster-name']
        bkp_metadata['created_at'] = datetime.now()
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
        print '{:30} \t {:30} {:30}'.format('Backup Name', 'Created At', 'Retention')
        for bkp in result:
            print '{:30} \t {:30} {:30}'.format(bkp['backup_name'], bkp['created_at'].strftime('%c %Z'),
                                                bkp['retention'].strftime('%c %Z'))

    def search_for_db(self, kdb_session, keyword):
        kdb_bkps = kdb_session['backups']
        result = kdb_bkps.find({'mongo_topology.shards.databases': keyword})
        print '{:30} \t {:30} {:30}'.format('Backup Name', 'Created At', 'Retention')
        for bkp in result:
            print '{:30} \t {:30} {:30}'.format(bkp['backup_name'],
                                                bkp['created_at'].strftime('%c %Z'),
                                                bkp['retention'].strftime('%c %Z')
                                                )

    def search_for_collection(self, kdb_session, keyword):
        kdb_bkps = kdb_session['backups']
        result = kdb_bkps.find({'mongo_topology.shards.collections': keyword})
        print '{:30} \t {:30} {:30}'.format('Backup Name', 'Created At', 'Retention')
        for bkp in result:
            print '{:30} \t {:30} {:30}'.format(bkp['backup_name'],
                                                bkp['created_at'].strftime('%c %Z'),
                                                bkp['retention'].strftime('%c %Z')
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

class SubCmdRecovery:
    def __init__(self, rst_spec=None):
        self.backup_name = rst_spec['backup-name']
        self.cluster_name = rst_spec['cluster-name']
        self.username = rst_spec['username']

    def restore(self, kdb_session):
        kdb_backup = kdb_session['backups']
        bkp2restore = kdb_backup.find_one({'backup_name': self.backup_name, 'cluster_name': self.cluster_name})

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
        
        kdb_netapp = kdb_session['ntapsystems']
        for svm in snaprestore_list.keys():
            svm_info = kdb_netapp.find_one({'svm-name': svm})
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
        delete_newers = kdb_backup.delete_many({'created_at': { '$gt': bkp2restore['created_at']}})
        logging.info('{} backups has been removed from the backup catalog.'.format(delete_newers.deleted_count))