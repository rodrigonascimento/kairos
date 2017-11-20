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

        # -- Getting a list of primaries across the cluster
        primaries_list = list()

        # -- Getting a list of secondaries across the cluster
        secondaries_list = list()

        if topology['cluster_type'] == 'replSet':
            for rs_member in topology['members']:
                if rs_member['stateStr'] == 'PRIMARY':
                    primaries_list.append(rs_member['name'][:rs_member['name'].find(':')])
                    logging.info(rs_member['name'][:rs_member['name'].find(':')] + ' server is the current primary member')
                elif rs_member['stateStr'] == 'SECONDARY':
                    secondaries_list.append(rs_member['name'][:rs_member['name'].find(':')])

        elif topology['cluster_type'] == 'sharded':
            for cs_member in topology['config_servers']:
                if cs_member['stateStr'] == 'PRIMARY':
                    primaries_list.append(cs_member['name'][:cs_member['name'].find(':')])
                    logging.info(cs_member['name'][:cs_member['name'].find(':')] + ' server is the current CSRS '
                                                                                    'primary member.')
                elif cs_member['stateStr'] == 'SECONDARY':
                    secondaries_list.append(cs_member['name'][:cs_member['name'].find(':')])

            for shard_replset in topology['shards']:
                for shard_member in shard_replset['shard_members']:
                    if shard_member['stateStr'] == 'PRIMARY':
                        primaries_list.append(shard_member['name'][:shard_member['name'].find(':')])
                        logging.info(shard_member['name'][:shard_member['name'].find(':')] + ' server is the current '
                                      + shard_replset['shard_name'] + ' primary member')
                    elif shard_member['stateStr'] == 'SECONDARY':
                        secondaries_list.append(shard_member['name'][:shard_member['name'].find(':')])

        data_members = list()
        for sec in secondaries_list:
            data_members.append(sec)

        for pri in primaries_list:
            data_members.append(pri)

        # -- Working with primaries_list to get the volumes used by each primary -- #
        svm_n_vols_layout = list()

        # --- Creating an array to store host_conn objects
        ssh2host = list()
        for host in data_members:
            ssh2host.append(HostConn(ipaddr=host, username=self.backup['username']))

        for host in ssh2host:
            svm_n_vols_layout.append(host.get_storage_layout(cluster_info['mongodb-mongod-conf']))
            host.close()

        # -- Creating snapshots objects list
        snapshot_list = list()
        cluster_sessions_list = list()
        for svm_n_vols in svm_n_vols_layout:
            per_server_cg = dict()
            per_server_cg['volume'] = list()
            for volume in svm_n_vols['volume_topology']:
                per_server_cg['volume'].append(volume['volume'])
                per_server_cg['svm-name'] = volume['svm-name'].strip()

            per_server_cg['snapname'] = self.backup['backup-name']
            per_server_cg['snap-type'] = 'cgsnap'
            per_server_cg['cg-timeout'] = 'relaxed'
            per_server_cg['primary_name'] = svm_n_vols['hostname_ip']

            snapshot_list.append(per_server_cg)

        # -- If sharded cluster, stopping the balancer before taking any snapshot
        if topology['cluster_type'] == 'sharded':
            mdbcluster.stop_balancer()

        # -- Creating CG snapshots per Primary
        # -- -- Connecting to kairos_repo to get the storage credentials
        kdb_netapp = kdb_session['ntapsystems']

        # -- -- For each Primary identified by the time of the backup, connects to its storage and shoot a snapshot
        for server in snapshot_list:
            svm_info = kdb_netapp.find_one({'svm-name': server['svm-name']})
            cs_svm = ClusterSession(svm_info['netapp-ip'], svm_info['username'], svm_info['password'], svm_info['svm-name'])
            cgsnap = Snapshot(server)
            result = cgsnap.cgcreate(cs_svm)
            if result[0] == 'passed':
                logging.info(server['primary_name'] + ' snapshot has been successfully taken.')
            else:
                #TODO: Rollback backup operation by deleting other volumes snapshots
                logging.error(server['primary_name'] + ' snapshot has failed.')
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
        bkp_metadata['svms_n_vols'] = svm_n_vols_layout
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
        for svm_n_vol in bkp2delete['svms_n_vols']:
            for vol in svm_n_vol['volume_topology']:
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
    def __init__(self):
        pass



