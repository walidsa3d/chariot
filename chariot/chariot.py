#!/bin/python
# -*- coding: utf-8 -*-
from __future__ import division
import sys
import os

import boto
import gzip
import logging
import psycopg2
import math

from boto.s3.key import Key
from retrying import retry
from subprocess import check_call, CalledProcessError
from data_to_redshift import EXPORT_TABLES_DEV

# posgtres credentials
DB_USER = ''
DB_Name = ''
DB_HOST = ''
PASSWORD = ''

# aws s3 credentials
AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
AWS_REGION = 'eu-central-1'

# aws redshift db credentials
RDB_USER = ''
RD_NAME = ''
R_HOST = ''
R_PASSWORD = ''
R_PORT = '5439'
# other
CSV_OUTPUT_DIR = 'csv_dir'
GZIP_OUTPUT_DIR = 'gzip_dir'


def init_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('/tmp/prdshift.log')
    console = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[%(asctime)s]-%(levelname)s-%(message)s',
                                  datefmt='%a, %d %b %Y %H:%M:%S')
    fh.setFormatter(formatter)
    console.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(console)
    return logger


def get_s3_connection():
    conn = boto.s3.connect_to_region(AWS_REGION,
                                     aws_access_key_id=AWS_ACCESS_KEY,
                                     aws_secret_access_key=AWS_SECRET_KEY,
                                     is_secure=True)
    return conn


def get_db_connection():
    conn = psycopg2.connect(database=DB_Name,
                            user=DB_USER,
                            host=DB_HOST,
                            password=PASSWORD)
    return conn


def s3_dir_upload(conn, bucket_name, rootdir):
    empty_bucket(conn, bucket_name)
    for fname in os.listdir(rootdir):
        fpath = os.path.join(rootdir, fname)
        log.info('Uploading %s' % fname)
        s3upload(conn, bucket_name, (fname, fpath))


def split_file(filename, filepath):
    chunk_size = '200M'
    file_size = os.path.getsize(filepath)
    if file_size > 200*1024*1024:
        # filename = os.path.join(CSV_OUTPUT_DIR, filename)
        command = 'split --line-bytes {} --additional-suffix=.csv --numeric-suffix {} {}_'.format(
            chunk_size, filepath, filepath)
        try:
            check_call(command, shell=True)
            os.remove(filepath)
        except CalledProcessError:
            log.exception('Error Splitting CSV File')
            raise


def export_to_csv(connection, tablename, export_query, outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    cur = connection.cursor()
    filename = '{}.csv'.format(tablename)
    filepath = os.path.join(outputdir, filename)
    outputquery = "COPY ({}) TO STDOUT WITH DELIMITER '|' CSV FORCE QUOTE *".format(export_query)
    with open(filepath, 'w') as f:
        cur.copy_expert(outputquery, f)
    return filename, filepath


def compress_file(filename, filepath, outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    archive_name = filename+'.gz'
    archive_path = os.path.join(outputdir, archive_name)
    with open(filepath) as f_in, gzip.open(archive_path, 'wb') as f_out:
        f_out.writelines(f_in)
    # os.remove(filepath)
    return archive_name, archive_path


def empty_dir(dirpath):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    for f in os.listdir(dirpath):
        fpath = os.path.join(dirpath, f)
        os.unlink(fpath)


def empty_bucket(conn, bucket_name):
    bucket = conn.get_bucket(bucket_name)
    for key in bucket.list():
        key.delete()


@retry(stop_max_attempt_number=5)
def s3upload(conn, bucket_name, file):
    filename, filepath = file
    bucket = conn.get_bucket(bucket_name)
    k = Key(bucket)
    if not bucket.get_key(filename):
        k.key = filename
        k.set_contents_from_filename(filepath)
    return 's3://{}/{}'.format(bucket_name, filename)


def s3_to_redshift(tablename, query_type, staging_query, chain_id, s3path):
    '''copy s3 file contents to redshift table'''
    # 's3://path/to/dump.csv'
    staging_table = 'staging_' + tablename + '_' + str(chain_id)
    conn = psycopg2.connect(database=RD_NAME, user=RDB_USER, host=R_HOST, password=R_PASSWORD, port=R_PORT)
    cur = conn.cursor()
    copy_query = """
                CREATE TABLE IF NOT EXISTS {} (LIKE {});
                TRUNCATE TABLE {};
                COPY {}
                FROM '{}' WITH CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
                GZIP delimiter '|'
                REMOVEQUOTES
                BLANKSASNULL
                ACCEPTANYDATE
                ACCEPTINVCHARS
                DATEFORMAT 'auto'
                TIMEFORMAT 'auto'
                COMPUPDATE ON
                MAXERROR 100
                region as '{}';""".format(staging_table, tablename, staging_table, staging_table, s3path, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION)
    cur.execute(copy_query)
    conn.commit()
    log.info('COPY table %s successfull' % tablename)
    upsert_query = """{};""".format(staging_query)
    cur.execute(upsert_query)
    conn.commit()
    log.info('UPSERT table %s successfull' % tablename)
    conn.close()


def get_or_create_bucket(conn, bucket_name):
    try:
        conn.get_bucket(bucket_name)
    except Exception as e:
        conn.create_bucket(bucket_name, location=AWS_REGION)
    bucket_path = 's3://{}'.format(bucket_name)
    return bucket_name, bucket_path


def run(chain_id):
    global log
    log = init_logger()
    csv_chain_dir = os.path.join(CSV_OUTPUT_DIR, chain_id)
    gzip_chain_dir = os.path.join(GZIP_OUTPUT_DIR, chain_id)
    log.info('Connecting to Postgres Database')
    conn = get_db_connection()
    log.info('Connecting to S3')
    s3_conn = get_s3_connection()
    bucket_name, bucket_path = get_or_create_bucket(s3_conn, 'mcchain-'+chain_id)
    for t in EXPORT_TABLES_DEV:
        log.info('Exporting table %s' % t['dest_table'])
        empty_dir(csv_chain_dir)
        empty_dir(gzip_chain_dir)
        table_name, query_type, staging_query, export_query = t['dest_table'], t['query_type'], t[
            'staging_query'] % (chain_id, chain_id, chain_id), t['export_query'] % (chain_id, chain_id, chain_id)
        csvfilename, csvfilepath = export_to_csv(conn, table_name, export_query, csv_chain_dir)
        split_file(csvfilename, csvfilepath)
        for filename in os.listdir(csv_chain_dir):
            filepath = os.path.join(csv_chain_dir, filename)
            compress_file(filename, filepath, gzip_chain_dir)
        s3_dir_upload(s3_conn, bucket_name, gzip_chain_dir)
        log.info('All files uploaded')
        log.info('Copying to Redshift')
        s3_to_redshift(table_name, query_type, staging_query, chain_id, bucket_path)
        break
    conn.close()
    s3_conn.close()

if __name__ == '__main__':
