#!/usr/bin/env python2

import logging
import pymongo
import pymongo.errors
from kairoslib.catalog import Catalog

LOGGER = logging.getLogger(__name__)


class ArchTempData:
    def __init__(self, arch_repo_uri=None, arch_repo_name=None, source_cluster_name=None, begin_from=None, upto=None,
                 temp_coll=None, arch_queue=None):
        self.source_cluster_name = source_cluster_name
        self.begin_from = begin_from
        self.until = upto
        self.temp_coll = temp_coll
        self.arch_repo_uri = arch_repo_uri
        self.arch_repo_name = arch_repo_name
        self.arch_queue = arch_queue
        self.op_arch_sess = None

    def create_temp_data(self):
        self.op_arch_sess = Catalog(repo_uri=self.arch_repo_uri, repo_name=self.arch_repo_name)
        self.op_arch_sess.connect()

        pipeline = [
            {
                '$match': {
                    '$and': [{'created_at': {'$gte': self.begin_from}}, {'created_at': {'$lte': self.until}}]
                }
            },
            {
                '$sort': {'_id': 1}
            },
            {
                '$group': {
                    '_id': {'dbname': '$ns.db', 'collname': '$ns.coll', 'doc_id': '$documentKey._id'},
                    'ops': {
                        '$push': {
                            'op_id': '$_id',
                            'created_at': '$created_at',
                            'op_type': '$operationType',
                            'full_doc': '$fullDocument'
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'lastOp': {'$arrayElemAt': ['$ops', -1]}
                }
            },
            {
                '$out': self.temp_coll
            }
        ]

        try:
            self.op_arch_sess.run_aggregation(coll_name=self.source_cluster_name, aggr_pipeline=pipeline)
            LOGGER.info('Temporary collection created according to the specified date/time boundaries')
            self.op_arch_sess.close()
        except:
            LOGGER.error('Failed to create temporary collection with the required data for recovery.')
            self.op_arch_sess.close()
            exit(1)

    def read_temp_data(self):
        self.op_arch_sess = Catalog(repo_uri=self.arch_repo_uri, repo_name=self.arch_repo_name)
        self.op_arch_sess.connect()

        temp_data = self.op_arch_sess.find_all(coll_name=self.temp_coll, query={}, ordered=[('_id', pymongo.ASCENDING)])
        # iter cursor and ingest data into the queue
        [self.arch_queue.put(operation) for operation in temp_data]
        self.op_arch_sess.close()

    def destroy_temp_data(self):
        self.op_arch_sess = Catalog(repo_uri=self.arch_repo_uri, repo_name=self.arch_repo_name)
        self.op_arch_sess.connect()

        try:
            self.op_arch_sess.drop_collection(coll_name=self.temp_coll)
            self.op_arch_sess.close()
            LOGGER.info('Temporary collection {} has been dropped.'.format(self.temp_coll))
        except:
            LOGGER.error('Failed to drop temporary collection {}.'.format(self.temp_coll))
            exit(1)
