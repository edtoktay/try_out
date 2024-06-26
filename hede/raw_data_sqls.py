GET_EXISTING_TAGS = '''
select id, tag_name from tags
'''

INSERT_NEW_TAG = '''
insert into tags(tag_name, processes)
values(%(tagName)s, false)
returning id
'''

START_NEW_BATCH_PARTITION = '''
insert into batches(batch_id, product_id, start_time, "PARTITION") 
values 
  (%(batchName)s,
   %(productName)s,
   %(startTime)s,
   %(partition)s)
returning id
'''

EXISTS_TIMESTAMP_VALUE = '''
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

GET_LATEST_BATCH_PARTITION = '''
select * from batches where batch_id = %(batchName)s
order by "PARTITION" desc
fetch first 1 row only
'''

GET_BATCH_PARTITION_BY_ID = '''
select * from batches where id = %(id)s
'''

INSERT_NEW_RAW_DATA = '''
insert into raw_tag_read (
  batch_id, 
  product_id, 
  tag_id, 
  "TIMESTAMP", 
  "VALUE", 
  "STATUS"
) 
values 
  (%(batchName)s,
   %(productName)s,
   %(tagName)s,
   %(timestamp)s,
   %(value)s,
   %(status)s) 
returning id
'''

INSERT_RAW_BATCH_REL = '''
insert into raw_read_batch_rel(raw_id, batch_id)
values(%(rawId)s, %(batchId)s)
'''

INSERT_RAW_TAG_REL = '''
insert into raw_read_tag_rel(raw_id, tag_id)
values(%(rawId)s, %(tagId)s)
'''

FINISH_BATCH_PARTITION = '''
update batches 
set end_time = %(endTime)s
where id = %(id)s
'''

GET_CONFIGURATIONS_OF_TAG = '''
select 
  tc.id as configuration_id, 
  t1.id as master_id, 
  t1.tag_name as master_name, 
  t2.id as error_id, 
  t2.tag_name as error_name 
from 
  tag_configurations tc 
  inner join tags t1 on tc.master_tag_id = t1.id 
  inner join tags t2 on tc.error_tag_id = t2.id 
where 
  t1.processes = true 
  and t2.processes = true 
  and (
    t1.tag_name = %(tagName)s 
    or t2.tag_name = %(tagName)s
  )
'''
