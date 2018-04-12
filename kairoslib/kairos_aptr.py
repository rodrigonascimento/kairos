#!/usr/bin/env python2

import logging
import pymongo
import pymongo.errors
import threading
import daemon
import daemon.pidfile
from datetime import datetime
from kairoslib.catalog import Catalog

LOGGER = logging.getLogger(__name__)


class Producer(threading.Thread):
    def __init__(self, cluster_name=None, mongodb_uri=None, database=None, collection=None, archiver_repo_uri=None,
                 archiver_repo_dbname=None):
        threading.Thread.__init__(self)
        self.cluster_name = cluster_name
        self.mongodb_uri = mongodb_uri
        self.database = database
        self.collection = collection

        self.archiver_repo_uri = archiver_repo_uri
        self.archiver_repo_dbname = archiver_repo_dbname

        self.archiver_repo_sess = Catalog(repo_uri=self.archiver_repo_uri, repo_name=self.archiver_repo_dbname)
        self.archiver_repo_sess.connect()

        l_invalid_op = self.archiver_repo_sess.find_one(coll_name=self.cluster_name,
                                                        query={'ns.db': self.database,
                                                               'ns.coll': self.collection,
                                                               'operationType': 'invalidate'},
                                                        ordered=[('_id', pymongo.DESCENDING)])

        l_valid_op = self.archiver_repo_sess.find_one(coll_name=self.cluster_name,
                                                      query={'ns.db': self.database, 'ns.coll': self.collection},
                                                      ordered=[('_id', pymongo.DESCENDING)])

        self.mongo_sess = pymongo.MongoClient(self.mongodb_uri, connect=True)
        db = self.mongo_sess[self.database]
        self.watching_collec = db[self.collection]

        if l_invalid_op is None:
            if l_valid_op is None:
                self.collec_cursor = self.watching_collec.watch(full_document='updateLookup')
            else:
                self.collec_cursor = self.watching_collec.watch(resume_after=l_valid_op['_id'],
                                                                full_document='updateLookup')
        else:
                self.collec_cursor = self.watching_collec.watch(full_document='updateLookup')

    def run(self):
        while True:
            try:
                op = next(self.collec_cursor)
                if op['operationType'] == 'invalidate':
                    op['ns'] = dict()
                    op['ns']['db'] = self.database
                    op['ns']['coll'] = self.collection
                op['created_at'] = datetime.now()
                self.archiver_repo_sess.add(coll_name=self.cluster_name, doc=op)
            except (StopIteration, pymongo.errors.OperationFailure):
                db = self.mongo_sess[self.database]
                self.watching_collec = db[self.collection]
                self.collec_cursor = self.watching_collec.watch(full_document='updateLookup')


class AppKairosAPTR:
    def __init__(self, cluster_name=None, database_name=None, collections=None, mongodb_uri=None, archiver_name=None,
                 archive_repo_uri=None, archive_repo_name=None):
        self.cluster_name = cluster_name
        self.archiver_name = archiver_name
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.collections = collections
        self.pidfilepath = '/tmp/kairosAPITR_' + self.cluster_name + '_' + self.archiver_name + '.pid'
        self.archive_repo_uri = archive_repo_uri
        self.archive_repo_name = archive_repo_name

        self.pidfile = daemon.pidfile.PIDLockFile(self.pidfilepath)
        self.context = daemon.DaemonContext(detach_process=True, pidfile=self.pidfile)

    def start(self):
        logging.info('{} has been started for cluster {}'.format(self.archiver_name, self.cluster_name))
        with self.context:
            producers = list()
            for coll in self.collections:
                producers.append(Producer(cluster_name=self.cluster_name, mongodb_uri=self.mongodb_uri,
                                          database=self.database_name, collection=coll,
                                          archiver_repo_uri=self.archive_repo_uri,
                                          archiver_repo_dbname=self.archive_repo_name))

            for producer in producers:
                producer.setDaemon(True)
                producer.start()

            while True:
                pass

    def get_pidfilename(self):
        return self.pidfilepath
