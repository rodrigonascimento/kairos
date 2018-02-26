#!/usr/bin/env python

import logging
import pymongo
import pymongo.errors
import threading
import daemon
import daemon.pidfile
from time import time
from datetime import datetime, timedelta
from kairoslib.catalog import Catalog

LOGGER = logging.getLogger(__name__)


class Producer(threading.Thread):
    def __init__(self, cluster_name=None, mongodb_uri=None, database=None, collection=None, retention=None,
                 archiver_repo_uri=None, archiver_repo_dbname=None):
        threading.Thread.__init__(self)
        self.cluster_name = cluster_name
        self.mongodb_uri = mongodb_uri
        self.database = database
        self.collection = collection
        self.retention = retention

        self.archiver_repo_uri = archiver_repo_uri
        self.archiver_repo_dbname = archiver_repo_dbname

        self.archiver_repo_sess = Catalog(repo_uri=self.archiver_repo_uri, repo_name=self.archiver_repo_dbname)
        self.archiver_repo_sess.connect()

        try:
            last_op = self.archiver_repo_sess.find_one(coll_name=self.cluster_name, query={'ns.db': self.database,
                                                                                           'ns.coll': self.collection},
                                                       ordered=[('_id', pymongo.DESCENDING)])

            self.mongo_sess = pymongo.MongoClient(self.mongodb_uri, connect=True)
            db = self.mongo_sess[self.database]
            self.watching_collec = db[self.collection]

            if last_op is None:
                self.collec_cursor = self.watching_collec.watch()
            else:
                self.collec_cursor = self.watching_collec.watch(resume_after=last_op['_id'])
        except (pymongo.errors.ConnectionFailure or pymongo.errors.OperationFailure), e:
            LOGGER.error(e)
            exit(1)


    def run(self):
        while True:
            try:
                for op in self.collec_cursor:
                    op['op_expires_at'] = self._calc_retention(retention=self.retention, created_at=datetime.now())
                    self.archiver_repo_sess.add(coll_name=self.cluster_name, doc=op)
            except pymongo.errors.PyMongoError, e:
                LOGGER.error(e)

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


class AppKairosAPTR:
    def __init__(self, cluster_name=None, database_name=None, collections=None, mongodb_uri=None, retention=None,
                 archiver_name=None, kairoscfg=None):
        self.cluster_name = cluster_name
        self.archiver_name = archiver_name
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.collections = collections
        self.retention = retention
        self.pidfilepath = '/tmp/kairosAPITR_' + self.cluster_name + '_' + self.archiver_name + '.pid'
        self.kcfg = kairoscfg

        self.pidfile = daemon.pidfile.PIDLockFile(self.pidfilepath)
        self.context = daemon.DaemonContext(detach_process=True, pidfile=self.pidfile)

    def start(self):
        with self.context:
            producers = list()
            for coll in self.collections:
                producers.append(Producer(cluster_name=self.cluster_name, mongodb_uri=self.mongodb_uri,
                                          database=self.database_name, collection=coll, retention=self.retention,
                                          archiver_repo_uri=self.kcfg['kairos-oplog-archive']['archiver-uri'],
                                          archiver_repo_dbname=self.kcfg['kairos-oplog-archive']['archiver-dbname']))

            for producer in producers:
                producer.setDaemon(True)
                producer.start()

            while True:
                pass

    def get_pidfilename(self):
        return self.pidfilepath