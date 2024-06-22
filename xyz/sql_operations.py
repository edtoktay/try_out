#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 16:42:02 2024

@author: deniz.toktay
"""
import psycopg2
import sqls

connection = psycopg2.connect(database='postgres_db',
                              user='postgres',
                              password='p.postgres',
                              host='172.16.238.10',
                              port=5432)
connection.autocommit = True
cursor = connection.cursor()


def get_tag_configurations():
    cursor.execute(sqls.get_tag_configurations)
    confs = cursor.fetchall()
    configs = []
    for cfg in confs:
        configs.append({'ConfigId': cfg[0], 
                        'MasterTagId': cfg[1],
                        'MasterTagName': cfg[2], 
                        'ErrorTagId': cfg[3], 
                        'ErrorTagName': cfg[4]})
    return configs


def get_configurations_of_tag(tagName):
    cursor.execute(sqls.get_tag_configurations, {'tagName': tagName})
    cfg = cursor.fetchone()
    return {'ConfigId': cfg[0], 
            'MasterTagId': cfg[1],
            'MasterTagName': cfg[2], 
            'ErrorTagId': cfg[3], 
            'ErrorTagName': cfg[4]} if cfg is not None else None


def get_active_tags():
    cursor.execute(sqls.get_active_tags)
    tags = cursor.fetchall()
    active_tags = []
    for tag in tags:
        active_tags.append({'id': tag[0], 'tagName': tag[1]})
    return active_tags


def get_latest_batch_partition(batch_name):
    cursor.execute(sqls.get_latest_batch_partition, {'batchId': batch_name})
    batch = cursor.fetchone()
    return {
        'id': batch[0],
        'batchId': batch[1],
        'product': batch[2],
        'startTime': batch[3],
        'endTime': batch[4],
        'partition': batch[5],
    } if batch is not None else None


def finish_batch_partition(partition_id, end_time):
    cursor.execute(sqls.finish_batch_partition, (end_time, partition_id))
    pass


def start_new_batch_partition(batch_name, product, start_time):
    previous_partition = get_latest_batch_partition(batch_name)
    if previous_partition is None:
        cursor.execute(sqls.start_new_batch_partition,
                       (batch_name, product, start_time, 1))
    else:
        cursor.execute(sqls.start_new_batch_partition, (batch_name,
                       product, start_time,
                       previous_partition['partition'] + 1))
    return cursor.fetchone()[0]


def exists_timestamp_value(batch_id, tag_id, timestamp, value):
    cursor.execute(sqls.exists_timestamp_value, {
                   'batchId': batch_id, 'tagId': tag_id,
                   'timestamp': timestamp, 'value': value})
    return cursor.fetchone()[0]


def insert_new_raw_data(batch_id, product, tag_id, timestamp, value, status):
    cursor.execute(sqls.insert_new_raw_data, (batch_id,
                   product, tag_id, timestamp, value, status))
    return cursor.fetchone()[0]


def get_tag_id(tag_name):
    cursor.execute(sqls.get_tag_id, {'tagName': tag_name})
    return cursor.fetchone()[0]


def get_the_sum_of_production(batch_id, tag_id):
    cursor.execute(sqls.get_the_sum_of_production, {
                   'batchId': batch_id, 'tagId': tag_id})
    summation = cursor.fetchone()[0]
    return int(summation) if summation is not None else 0


def get_the_latest_interpolated_data(batch_id, tag_id):
    cursor.execute(sqls.get_latest_interpolated_data, {
                   'batchId': batch_id, 'tagId': tag_id})
    interpolated = cursor.fetchone()
    return {
        'id': interpolated[0],
        'timestamp': interpolated[1],
        'value': interpolated[2],
        'type': interpolated[3]
    } if interpolated is not None else None


def fix_raw_data(batch_id, tag_id, timestamp):
    cursor.execute(sqls.fix_raw_data, ('NEW', batch_id, tag_id, timestamp))
    ids_to_ignore = cursor.fetchall()
    if ids_to_ignore is not None and len(ids_to_ignore) > 0:
        update_raw_status(ids_to_ignore, 'NOT PROCESS')
    pass


def update_raw_status(raw_ids, status):
    cursor.execute(sqls.update_raw_status, (status, raw_ids))
    commit()
    pass


def get_raw_data_to_process(ids_to_fetch):
    cursor.execute(sqls.get_raw_data_to_process, (ids_to_fetch,))
    raws = cursor.fetchall()
    raw_data_to_process = []
    ids = []
    for raw in raws:
        raw_data_to_process.append(
            {'id': raw[0], 'timestamp': raw[1], 'value': raw[2]})
        ids.append(raw[0])
    '''
    if len(ids) > 0:
        update_raw_status(tuple(ids), 'IN PROGRESS')
    '''
    return raw_data_to_process


def insert_interpolated_data(timestamp, value, ty):
    cursor.execute(sqls.insert_interpolated_data, (timestamp, value, ty))
    return cursor.fetchone()[0]


def insert_interpolated_batch(interpolated_id, batch_id):
    cursor.execute(sqls.insert_interpolated_batch, (interpolated_id, batch_id))
    pass


def insert_interpolated_tag(interpolated_id, tag_id):
    cursor.execute(sqls.insert_interpolated_tag, (interpolated_id, tag_id))
    pass


def insert_boxed_data(start_time, end_time, total_start, total_end, error_start, error_end, size, errors, error_rate):
    cursor.execute(sqls.insert_boxed_data, (start_time, end_time, total_start,
                   total_end, error_start, error_end, size, errors, error_rate))
    return cursor.fetchone()[0]


def insert_boxed_batch(boxed_id, batch_id):
    cursor.execute(sqls.insert_boxed_batch, (boxed_id, batch_id))
    pass


def insert_boxed_tag(boxed_id, tag_id):
    cursor.execute(sqls.insert_boxed_tag, (boxed_id, tag_id))
    pass

def commit():
    connection.commit()
    pass


if __name__ == '__main__':
    pass
