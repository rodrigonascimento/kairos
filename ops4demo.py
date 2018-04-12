#!/usr/bin/env python2

import pymongo
import random
import time

def main():
    # connection to bkpdemo database
    conn = pymongo.MongoClient('mongodb://10.61.105.165:27017')
    bkpdemo_db = conn['bkpdemo']
    # collections
    collections = ['test1', 'test3']

    # (a) insert documents
    for collection in collections:
        print 'Inserting documents into collection %s' % collection
        num_id = 1
        while num_id <= 10000:
            # document to be inserted in the random collection
            doc = dict()
            doc['_id'] = num_id
            doc['name'] = 'kairos demo ' + str(num_id)
            bkpdemo_db[collection].insert_one(doc)
            num_id += 1

    # (b) update documents
    for collection in collections:
        print 'Updating documents on collection %s' % collection
        opcount = 0
        while opcount < 1000:
            doc_id = random.randrange(1, 10000)
            bkpdemo_db[collection].update_one(filter={'_id': doc_id}, update={'$set': {'desc': 'updated ops before delete'}})
            opcount += 1

    # (c) delete documents
    for collection in collections:
        print 'Deleting some documents on collection %s' % collection
        opcount = 0
        while opcount < 100:
            doc_id = random.randrange(1, 10000)
            bkpdemo_db[collection].delete_one(filter={'_id': doc_id})
            opcount += 1

    # (d) update documents
    for collection in collections:
        print 'Updating more documents on collection %s' %collection
        opcount = 0
        while opcount < 1000:
            doc_id = random.randrange(1, 10000)
            bkpdemo_db[collection].update_one(filter={'_id': doc_id}, update={'$set': {'desc': 'updated ops after delete'}})
            opcount += 1

    print '[ Done ]'

if __name__ == '__main__':
    main()