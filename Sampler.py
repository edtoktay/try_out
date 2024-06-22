from PandasOps import PandasOps
from SqlOperations import SqlOperations
from ResponseMapper import ResponseMapper
from Enums import DataStatus

ops = SqlOperations(
    db='postgres_db',
    user='postgres',
    password='p.postgres',
    host='172.16.238.10'
)

po = PandasOps()

data_frame = po.create_csv_dataframe('ED1108_1.csv')

print(data_frame)

exists_timestamp_value = '''
select 
  exists (
    select 
      1 
    from 
      raw_tag_read r 
    where 
      r.batch_id = %(batchName)s 
      and r.tag_id = %(tagName)s 
      and r."TIMESTAMP" = %(timestamp)s 
      and r."VALUE" = %(value)s
  )
'''
get_latest_batch_partition = '''
select * from batches where batch_id = %(batchId)s
order by "PARTITION" desc
fetch first 1 row only
'''
insert_new_raw_data = '''
INSERT INTO raw_tag_read (
  batch_id, 
  product_id, 
  tag_id, 
  "TIMESTAMP", 
  "VALUE", 
  "STATUS"
) 
VALUES 
  (%(batchName)s,
   %(productName)s,
   %(tagName)s,
   %(timestamp)s,
   %(value)s,
   %(status)s) 
RETURNING id
'''
finish_batch_partition = '''
update batches 
set end_time = %(endTime)s
where id = %(id)s
'''

start_new_batch_partition = '''
INSERT INTO batches(batch_id, product_id, start_time, "PARTITION") 
VALUES 
  (%(batchName)s,
   %(productName)s,
   %(startTime)s,
   %(partition)s)
RETURNING id
'''
new_records = []
cp_mapper = ResponseMapper(
    ['id', 'batchName', 'product', 'startTime', 'endTime', 'partition'])
requires_new_partition = False
for index, row in data_frame.iterrows():
    batch_name = row['BATCH_ID']
    product = row['PRODUCT_ID']
    tag_name = row['TAG_ID']
    timestamp = row['TIMESTAMP']
    value = row['VALUE']
    is_exists = ops.query_with_return(exists_timestamp_value, {
        'batchName': batch_name,
        'tagName': tag_name,
        'timestamp': timestamp,
        'value': value
    })[0][0]
    cp = cp_mapper.map(ops.query_with_return(get_latest_batch_partition,  {
        'batchId': batch_name
    }))
    current_partition = cp[0] if cp is not None and len(cp) == 1 else None
    if not ops.query_with_return(exists_timestamp_value, {
        'batchName': batch_name,
        'tagName': tag_name,
        'timestamp': timestamp,
        'value': value
    })[0][0]:
        if value == 0:
            ops.query_without_return(insert_new_raw_data, {
                'batchName': batch_name,
                'productName': product,
                'tagName': tag_name,
                'timestamp': timestamp,
                'value': value,
                'status': DataStatus.IGNORE.value
            })
            requires_new_partition = True
            if current_partition is not None and current_partition['endTime'] is None:
                ops.query_without_return(finish_batch_partition, {
                    'id': current_partition['id'],
                    'endTime': timestamp
                })
        else:
            ops.query_without_return(start_new_batch_partition, {
                'batchName': batch_name,
                'productName': product,
                'startTime': timestamp,
                'partition': current_partition['partition'] + 1 if current_partition is not None else 1
            })
            current_partition = cp_mapper.map(ops.query_with_return(get_latest_batch_partition,  {
                'batchId': batch_name
            }))
            requires_new_partition = False
            inserted_id = ops.query_with_return(insert_new_raw_data, {
                'batchName': batch_name,
                'productName': product,
                'tagName': tag_name,
                'timestamp': timestamp,
                'value': value,
                'status': DataStatus.NEW.value
            })
            new_records.append(inserted_id[0])

get_raw_data_to_process = '''
select 
  id,
  "TIMESTAMP",
  "VALUE"
from 
  raw_tag_read 
where 
  "STATUS" = 'NEW' 
  and id IN %(ids)s
order by "TIMESTAMP" asc
'''

rm = ResponseMapper(['id', 'timestamp', 'value'])
raw_data = rm.map(ops.query_with_return(sql=get_raw_data_to_process, values={
    'ids': tuple(new_records)
}))

print(raw_data)

df = po.create_dictionary_dataframe(dictionary_list=raw_data)

print(len(df))

df = po.clear_duplicates(
    data_frame=df, field_name='timestamp', sort_values_list=['timestamp', 'value'])
print(len(df))

df = po.clear_duplicates(data_frame=df, field_name='value',
                         sort_values_list=['timestamp', 'value'])
print(len(df))

values = df['value'].tolist
timestamps = df['timestamp'].tolist
interpolated_df = po.create_timeseries_datafarame(time_serie=po.interpolated_time_serie(
    values=values, timestamps=timestamps), columns=['value'])

print(len(interpolated_df))

print(interpolated_df)
