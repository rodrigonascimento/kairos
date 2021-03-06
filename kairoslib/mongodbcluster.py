#!/usr/bin/env python2

import logging
from sys import exit
from time import sleep
from pymongo import MongoClient, errors, database


class MongoDBCluster:
    def __init__(self, mongodb_uri=None):
        try:
            self.conn = MongoClient(mongodb_uri, connect=True)
        except errors.ConnectionFailure, e:
            logging.error(e.message)
            exit(1)

    def _run_command(self, cmd=None):
        return self.conn.admin.command(cmd)

    def is_balancer_in_round(self):
        config_db = self.conn['config']
        mongos_coll = config_db['mongos']
        result_mongos = mongos_coll.find_one({'up': { '$gt': 0 }})

        mongos_uri = 'mongodb://' + result_mongos['_id']

        mongos = MongoDBCluster(mongos_uri)

        while True:
            status = mongos._run_command('balancerStatus')
            if not status['inBalancerRound']:
                logging.info('Balancer is not in a round.')
                break
            else:
                logging.warning('Balancer is moving a chunk.')
                sleep(1)

        return status['inBalancerRound']

    def stop_balancer(self):
        configdb = self.conn['config']
        settings = configdb['settings']
        try:
            settings.update_one({'_id': 'balancer'}, {'$set': {'stopped': True}}, upsert=True).modified_count
            self.is_balancer_in_round()
        except Exception as e:
            logging.error(e.message)
            exit(1)

    def start_balancer(self):
        configdb = self.conn['config']
        settings = configdb['settings']
        try:
            settings.update_one({'_id': 'balancer'}, {'$set': {'stopped': False}}, upsert=True).modified_count
        except Exception as e:
            logging.error(e.message)
            exit(1)

    def get_topology(self):
        databases = self.conn.admin.command("listDatabases")['databases']
        for db in databases:
            if db['name'] == 'config':
                dbconfig = self.conn['config']
                shards_coll = dbconfig['shards']
                if shards_coll.count() > 0:
                    cluster_type = 'sharded'
                    break
            else:
                cluster_type = 'replSet'

        cluster_topology = dict()
        cluster_topology['cluster_type'] = cluster_type
        if cluster_type == 'replSet':
            cluster_topology['members'] = list()
            for member in self.conn.admin.command("replSetGetStatus")['members']:
                doc = dict()
                doc['_id'] = member['_id']
                doc['name'] = member['name']
                doc['stateStr'] = member['stateStr']
                cluster_topology['members'].append(doc)
                if member['stateStr'] == 'PRIMARY':
                    cluster_topology['databases'] = self.get_dbs_collections()

            return cluster_topology
        elif cluster_type == 'sharded':
            cluster_topology['config_servers'] = list()
            cluster_topology['shards'] = list()

            # -- Getting config server replica set info
            for member in self.conn.admin.command("replSetGetStatus")['members']:
                doc = dict()
                doc['_id'] = member['_id']
                doc['name'] = member['name']
                doc['stateStr'] = member['stateStr']
                cluster_topology['config_servers'].append(doc)

            # -- Getting per shard info
            configdb = self.conn['config']
            shards_col = configdb['shards']
            for shard in shards_col.find():
                shard_uri = 'mongodb://' + shard['host'][shard['host'].find('/')+1:]
                shard_replSet = MongoDBCluster(shard_uri)
                doc = dict()
                doc['shard_name'] = shard['_id']
                doc['shard_members'] = shard_replSet.get_topology()['members']
                doc['databases'] = shard_replSet.get_dbs_collections()
                cluster_topology['shards'].append(doc)
            return cluster_topology

    def get_databases(self):
        dbs = self.conn.admin.command("listDatabases")['databases']
        db_list = list()
        for db in dbs:
            if (db['name'] == 'config') or (db['name'] == 'admin') or (db['name'] == 'local'):
                continue
            else:
                db_list.append(db['name'])
        return db_list

    def get_collections(self, db_name=None):
        coll_list = list()
        collection_list = database.Database(client=self.conn, name=db_name)
        for collection in collection_list.collection_names():
            coll_list.append(collection)
        return coll_list

    def get_dbs_collections(self):
        dbs = self.conn.admin.command('listDatabases')['databases']
        coll_in_db = dict()
        for db in dbs:
            if (db['name'] == 'config') or (db['name'] == 'admin') or (db['name'] == 'local'):
                continue
            else:
                coll_in_db[db['name']] = list()
                coll_cursor = database.Database(client=self.conn, name=db['name'])
                for collection in coll_cursor.collection_names():
                    coll_in_db[db['name']].append(collection)
        return coll_in_db

    def get_replset_config(self):
        result = self._run_command('replSetGetConfig')
        return result

    def update_replset_config(self, replset_reconfig=None):
        result = self.conn.admin.command('replSetReconfig', replset_reconfig, force=True)
        return result

    def delete_doc(self, dbname=None, collection=None, delete_filter=None):
        db = self.conn[dbname]
        coll = db[collection]
        try:
            result = coll.delete_one(filter=delete_filter).deleted_count
            return result
        except:
            print 'Failed to delete the document on collection {}.'.format(collection)
            exit(1)

    def update_doc(self, dbname=None, collection=None, update_filter=None, update_doc=None):
        db = self.conn[dbname]
        coll = db[collection]
        try:
            result = coll.update_one(filter=update_filter, update=update_doc).modified_count
            return result
        except:
            print 'Failed to update document on collection {}.'.format(collection)
            exit(1)

    def get_mongos_uri(self):
        configdb = self.conn['config']
        coll_mongos = configdb['mongos']
        result = coll_mongos.find({}, {'_id': 1})
        mongos_uri = 'mongodb://'
        for mongos in result:
            mongos_uri += mongos['_id'] + ','
        return mongos_uri[:-1]