#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
from boto.s3.key import Key
import boto
from multiprocessing.pool import ThreadPool


class S3Uploader(object):

    def __init__(self,
                 access_key=None,
                 secret_key=None,
                 region=None):
        self.access_key = access_key
        self.aws_secret_key = secret_key
        self.aws_region = region
        try:
            self.conn = self.get_connection()
        except:
            print "Cannot connect to S3"
            raise

    def get_connection(self):
        connection = boto.s3.connect_to_region(self.aws_region,
                                               aws_access_key_id=self.access_key,
                                               aws_secret_key=self.secret_key,
                                               is_secure=True)
        return connection

    def upload_file(self, filename, filepath, bucket_name):
        bucket = self.conn.get_bucket(bucket_name)
        k = Key(bucket)
        if not bucket.get_key(filename):
            k.key = filename
            k.set_contents_from_filename(filepath)
        return self.get_s3_uri(bucket_name, filename)

    @staticmethod
    def get_s3_uri(bucket_name, key_name=''):
        return 's3://{}/{}'.format(bucket_name, key_name)

    def upload_directory(self, dirpath, bucket_name):
        for fname in os.listdir(dirpath):
            fpath = os.path.join(dirpath, fname)
            self.upload_file(fname, fpath, bucket_name)

    def parrallel_upload(self, dirpath, bucket_name, max_threads=5):
        pool = ThreadPool(max_threads)
        for fname in os.listdir(dirpath):
            fpath = os.path.join(dirpath, fname)
            res = pool.apply_async(self.upload_file, args=(fname, fpath, bucket_name))
        return res.get()

    def multi_upload(self, bucket, filename, filepath):
        MAX_SIZE = 5*1000*1000*1000
        PART_SIZE = 500*1000*1000
        filesize = os.path.getsize(filepath)
        if filesize > MAX_SIZE:
            print "multipart upload"
            mp = bucket.initiate_multipart_upload(filename)
            fp = open(filepath, 'rb')
            fp_num = 0
            while (fp.tell() < filesize):
                fp_num += 1
                print "uploading part %i" % fp_num
                mp.upload_part_from_file(fp, fp_num, size=PART_SIZE)
            mp.complete_upload()

    def create_bucket(self, bucket_name):
        pass
