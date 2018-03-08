#!/usr/bin/env python


import json
import logging
from datetime import datetime, timedelta
from daemon import runner
from time import sleep
from kairoslib.catalog import Catalog
from kairoslib.ontap import ClusterSession, Snapshot

def load_config_file(config_file=None):
    with open(config_file, 'r') as cfgfile:
        kconfig = json.load(cfgfile)
        return kconfig

class Backup_Grim_Reaper:
    def __init__(self, catalog_sess=None, check_interval=None):
        self.check_interval = check_interval
        self.catalog = catalog_sess

    def check_expired_backups(self):
        check_timenow = datetime.now() - timedelta(seconds=self.check_interval)
        results = self.catalog.find_all(coll_name='backups', query={'retention': {'$lte': check_timenow}})
        return results

    def delete_expired_backups(self, expired_backups=None):
        if expired_backups.count() > 0:
            for backup in expired_backups:
                delete_list = dict()
                if backup['mongo_topology']['cluster_type'] == 'replSet':
                    for rs_member in backup['mongo_topology']['members']:
                        if rs_member['stateStr'] == 'PRIMARY' or rs_member['stateStr'] == 'SECONDARY':
                            for vol in rs_member['storage_info']['volume_topology']:
                                if vol['svm-name'] not in delete_list.keys():
                                    delete_list[vol['svm-name']] = list()
                                    delete_list[vol['svm-name']].append(vol['volume'])
                                else:
                                    delete_list[vol['svm-name']].append(vol['volume'])
                elif backup['mongo_topology']['cluster_type'] == 'sharded':
                    for cs_member in backup['mongo_topology']['config_servers']:
                        if cs_member['stateStr'] == 'PRIMARY' or cs_member['stateStr'] == 'SECONDARY':
                            for vol in cs_member['storage_info']['volume_topology']:
                                if vol['svm-name'] not in delete_list.keys():
                                    delete_list[vol['svm-name']] = list()
                                    delete_list[vol['svm-name']].append(vol['volume'])
                                else:
                                    delete_list[vol['svm-name']].append(vol['volume'])

                    for shard_replset in backup['mongo_topology']['shards']:
                        for shard_member in shard_replset['shard_members']:
                            if shard_member['stateStr'] == 'PRIMARY' or shard_member['stateStr'] == 'SECONDARY':
                                for vol in shard_member['storage_info']['volume_topology']:
                                    if vol['svm-name'] not in delete_list.keys():
                                        delete_list[vol['svm-name']] = list()
                                        delete_list[vol['svm-name']].append(vol['volume'])
                                    else:
                                        delete_list[vol['svm-name']].append(vol['volume'])

                for svm in delete_list.keys():
                    svm_info = self.catalog.find_one(coll_name='ntapsystems', query={'svm-name': svm})
                    cs_svm = ClusterSession(cluster_ip=svm_info['netapp-ip'], user=svm_info['username'],
                                            password=svm_info['password'], vserver=svm_info['svm-name']
                                            )
                    for volume in delete_list[svm]:
                        snap_spec = dict()
                        snap_spec['volume'] = volume
                        snap_spec['snapname'] = backup['backup_name']

                        snapshot = Snapshot(snap_spec)
                        delete_result = snapshot.delete(svm=cs_svm)
                        if delete_result[0] == 'passed':
                            logging.info('Snapshot {} on volume {} has been deleted for cluster {}.'.format(snap_spec['snapname'],
                                                                                                            snap_spec['volume'],
                                                                                                            backup['cluster_name']
                                                                                                            ))
                        else:
                            logging.error('Failed to delete snapshot {} on volume for cluster {}.'.format(snap_spec['snapname'],
                                                                                                            snap_spec['volume'],
                                                                                                            backup['cluster_name']
                                                                                                            ))
                remove_from_catalog = self.catalog.remove_one(coll_name='backups', query={'backup_name': backup['backup_name']})
                if remove_from_catalog > 0:
                    logging.info('Backup {} for cluster {} has been deleted successfuly.'.format(backup['backup_name'],
                                                                                                 backup['cluster_name']
                                                                                                 ))
                else:
                    logging.error('Failed to remove backup {} for cluster {} from kairos catalog.'.format(backup['backup_name'],
                                                                                                          backup['cluster_name']
                                                                                                          ))


class AppBackupGrimReaper():
    def __init__(self):
        self.stdin_path, self.stdout_path, self.stderr_path = ('/dev/null', '/dev/tty', '/dev/tty')
        self.pidfile_path = '/tmp/.bkpgrimreaper.pid'
        self.pidfile_timeout = 5
        self.kcfg = load_config_file(config_file='kairos.json')

    def run(self):
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', level=logging.INFO,
                            handlers=[logging.FileHandler(filename='bkp_grim_reaper.log'), logging.StreamHandler()])

        catalog = Catalog(repo_uri=self.kcfg['kairos-repo']['repo-uri'], repo_name=self.kcfg['kairos-repo']['db-name'])
        catalog.connect()

        bgr = Backup_Grim_Reaper(catalog_sess=catalog, check_interval=60)

        while True:
            bgr.delete_expired_backups(expired_backups=bgr.check_expired_backups())
            sleep(bgr.check_interval)

def main():
    appBGR = AppBackupGrimReaper()
    py_daemon = runner.DaemonRunner(appBGR)
    py_daemon.do_action()

if __name__ == '__main__':
    main()
