#!/usr/bin/env python

import logging
import pymongo
import pymongo.errors
import multiprocessing as mp
from time import time
from kairoslib.catalog import Catalog

LOGGER = logging.getLogger(__name__)


class RecoverConsumer(mp.Process):
    def __init__(self, arch_queue=None, dest_cluster_uri=None):
        self.dest_cluster = dest_cluster_uri
        self.arch_queue = arch_queue
        self.conn = None

    def run(self):
        # While using multiprocessing, pymongo doesn't recommend to open up a connection before a fork.
        # Opening a connection after fork
        try:
           self.conn = pymongo.MongoClient(self.dest_cluster)
        except pymongo.errors.ConnectionFailure, e:
           LOGGER.error(e)
           exit(1)
        while not self.arch_queue.empty():
            # Get a document from the queue
            recover_doc = self.arch_queue.get()
            self.arch_queue.task_done()
            # Set the database to the one specified in the recover document
            db = self.conn[recover_doc['_id']['dbname']]

            # Set the collection to the one specified in the recover document
            coll = db[recover_doc['_id']['collname']]

            # Perform the operation according to the op_type specified in the recover document
            if recover_doc['lastOp']['op_type'] == 'insert':
                coll.insert_one(document=recover_doc['lastOp']['full_doc'])
            elif recover_doc['lastOp']['op_type'] == 'update':
                coll.replace_one(filter={'_id': recover_doc['lastOp']['full_doc']['_id']},
                                 replacement=recover_doc['lastOp']['full_doc'], upsert=True)
            elif recover_doc['lastOp']['op_type'] == 'delete':
                coll.delete_one(filter={'_id': recover_doc['_id']['doc_id']})
