get_tag_configurations = '''
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
'''

get_configurations_of_tag = '''
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

get_active_tags = 'select t.id, t.tag_name from tags t where t.processes = true'
get_latest_batch_partition = '''
select * from batches where batch_id = %(batchId)s
order by "PARTITION" desc
fetch first 1 row only
'''

finish_batch_partition = '''
update batches 
set end_time = %s
where id = %s
'''

start_new_batch_partition = '''
INSERT INTO batches(batch_id, product_id, start_time, "PARTITION") 
VALUES (%s, %s, %s, %s)
RETURNING id
'''

exists_timestamp_value = '''
select 
  exists (
    select 
      1 
    from 
      raw_tag_read r 
    where 
      r.batch_id = %(batchId)s 
      and r.tag_id = %(tagId)s 
      and r."TIMESTAMP" = %(timestamp)s 
      and r."VALUE" = %(value)s
  )
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
  (%s, %s, %s, %s, %s, %s) 
RETURNING id
'''

get_tag_id = 'select id from tags where tag_name = %(tagName)s'

get_the_sum_of_production = '''
select
	sum(r."VALUE")
from
	raw_interpolated r
inner join raw_interpolated_batches rb
on r.id = rb.raw_read_id
inner join raw_interpolated_tags rt
on r.id = rt.raw_read_id
where
	rb.batch_id = %(batchId)s
	and rt.tag_id = %(tagId)s
'''

get_latest_interpolated_data = '''
select * from raw_interpolated r
inner join raw_interpolated_batches rb
on
	r.id = rb.raw_read_id
inner join raw_interpolated_tags rt
on
	r.id = rt.raw_read_id
where
	rb.batch_id = %(batchId)s
	and rt.tag_id = %(tagId)s
	order by r."TIMESTAMP" asc fetch first 1 row only
'''

fix_raw_data = '''
select 
  id 
from 
  raw_tag_read 
where 
  "STATUS" = 'NEW' 
  and batch_id = %s 
  and tag_id = %s 
  and "TIMESTAMP" < %s
'''

get_raw_data_to_process = '''
select 
  id,
  "TIMESTAMP",
  "VALUE"
from 
  raw_tag_read 
where 
  "STATUS" = 'NEW' 
  and id IN %s
order by "TIMESTAMP" asc
'''

update_raw_status = '''
update 
  raw_tag_read 
set 
  "STATUS" = %s 
where 
  id in %s
'''

insert_interpolated_data = '''
INSERT INTO raw_interpolated ("TIMESTAMP", "VALUE", "TYPE") 
VALUES (%s, %s, %s)
RETURNING id
'''

insert_interpolated_batch = '''
INSERT INTO raw_interpolated_batches(raw_read_id, batch_id)
VALUES(%s, %s)
'''

insert_interpolated_tag = '''
INSERT INTO raw_interpolated_tags(raw_read_id, tag_id)
VALUES(%s, %s)
'''
insert_boxed_data = '''
INSERT INTO processed_base_boxes
(start_time, end_time, total_start, total_end, error_start, error_end, "SIZE", errors, error_rate)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING id
'''

insert_boxed_batch = '''
INSERT INTO processed_base_boxes_batches(processed_id, batch_id)
VALUES(%s, %s)
'''

insert_boxed_tag = '''
INSERT INTO processed_base_boxes_tags(processed_id, tag_id)
VALUES(%s, %s)
'''
get_interpolated_tags = '''
select distinct
  r."TIMESTAMP", 
  r."VALUE" 
from 
  raw_interpolated r 
where 
  r.id in %s 
order by 
  r."TIMESTAMP" asc
'''

if __name__ == '__main__':
    pass
