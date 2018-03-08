#!/usr/bin/env python

import logging
from pymongo import MongoClient
from pymongo import errors
from pymongo import ASCENDING, IndexModel
from pymongo.database import Database


class Catalog:
    def __init__(self, repo_uri=None, repo_name=None):
        self.repo_uri = repo_uri
        self.repo_name = repo_name
        self.session = None
        self.kairosdb = None

    def connect(self):
        try:
            mongodb_uri = self.repo_uri + '/' + self.repo_name
            self.session = MongoClient(mongodb_uri, connect=True)
            self.kairosdb = self.session[self.repo_name]
        except (errors.ConnectionFailure, errors.InvalidURI), e:
            logging.error(e)
            exit(1)

    def create(self):
        kairos_collections = ['mdbclusters', 'ntapsystems', 'clones', 'backups', 'oplogarchiver']
        db_kairos = Database(client=self.session, name=self.repo_name)
        for kc in kairos_collections:
            try:
                logging.info('Creating repository collection {}.'.format(kc))
                db_kairos.create_collection(name=kc)
            except errors.CollectionInvalid, e:
                logging.error(e)
                exit(1)

        coll_indexes = dict()
        coll_indexes['mdbclusters'] = IndexModel([('cluster_name', ASCENDING)],
                                                 name='idx_mdbcluster_cluster_name',
                                                 unique=True
                                                 )

        coll_indexes['ntapsystems'] = IndexModel([('netapp-ip', ASCENDING)],
                                                 name='idx_ntapsystems_netapp_ip',
                                                 unique=True
                                                 )

        coll_indexes['clones'] = IndexModel([('cluster_name', ASCENDING),
                                             ('clone_name', ASCENDING)],
                                            name='idx_clones_cluster_name_clone_name',
                                            unique=True
                                            )

        coll_indexes['backups'] = IndexModel([('cluster_name', ASCENDING),
                                              ('backup_name', ASCENDING)],
                                             name='idx_backups_cluster_name_backup_name',
                                             unique=True
                                             )

        coll_indexes['oplogarchiver'] = IndexModel([('cluster_name', ASCENDING),
                                              ('archiver_name', ASCENDING)],
                                             name='idx_oplogarch_cluster_name_archiver_name',
                                             unique=True
                                             )

        for coll_idx in coll_indexes.keys():
            try:
                logging.info('Creating index on repository collection {}.'.format(coll_idx))
                coll = db_kairos[coll_idx]
                coll.create_indexes([coll_indexes[coll_idx]])
            except errors.OperationFailure, e:
                logging.error(e)
                exit(1)

    def add(self, coll_name=None, doc=None):
        coll = self.kairosdb[coll_name]
        try:
            result = coll.insert_one(doc).inserted_id
            return result
        except errors.DuplicateKeyError, e:
            logging.error(e)
            return

    def edit(self, coll_name=None, query=None, update=None):
        coll = self.kairosdb[coll_name]
        coll.find_one_and_update(filter=query, update=update)

    def remove_one(self, coll_name=None, query=None):
        coll = self.kairosdb[coll_name]
        try:
            result = coll.delete_one(query)
            return result.deleted_count
        except errors.InvalidOperation:
            return -1

    def find_all(self, coll_name=None, query=None, ordered=None):
        coll = self.kairosdb[coll_name]
        if ordered is not None:
            return coll.find(query, sort=ordered)
        else:
            return coll.find(query)

    def find_one(self, coll_name=None, query=None, ordered=None):
        coll = self.kairosdb[coll_name]
        if ordered is not None:
            return coll.find_one(query, sort=ordered)
        else:
            return coll.find_one(query)

    def run_aggregation(self, coll_name=None, aggr_pipeline=None):
        coll = self.kairosdb[coll_name]
        return list(coll.aggregate(aggr_pipeline))

    def drop_collection(self, coll_name=None):
        coll = self.kairosdb[coll_name]
        coll.drop()

    def close(self):
        self.session.close()