GET_INTERPOLATED_VALUES_OF_TAG = '''
select 
  r."TIMESTAMP",
  r."VALUE"
from 
  raw_interpolated r 
  inner join raw_interpolated_batches_rel b on r.id = b.raw_interpolate_id 
  inner join raw_interpolated_tags_rel t on r.id = t.raw_interpolate_id 
where 
  b.batch_id = %(batchId)s 
  and t.tag_id = %(tagId)s 
  and r."TIMESTAMP" >= %(timestamp)s  
order by 
  r."TIMESTAMP" asc
'''

GET_LATEST_CALCULATED_INTERPOLATION = '''
select 
  r."TIMESTAMP",
  r."VALUE"
from 
  raw_interpolated r 
  inner join raw_interpolated_batches_rel b on r.id = b.raw_interpolate_id 
  inner join raw_interpolated_tags_rel t on r.id = t.raw_interpolate_id 
where 
  b.batch_id = %(batchId)s 
  and t.tag_id = %(tagId)s 
  and r."TIMESTAMP" >= %(startTime)s
  and r."TIMESTAMP" <= %(endTime)s 
order by 
  r."TIMESTAMP" asc
'''

INSERT_BASE_MOVING_AVG = '''
INSERT INTO base_moving_average
(start_time, end_time, total_start, total_end, error_start, error_end, "SIZE", errors, error_rate)
VALUES(
    %(startTime)s, 
    %(endTime)s, 
    %(totalStart)s, 
    %(totalEnd)s, 
    %(errorStart)s, 
    %(errorEnd)s, 
    %(size)s, 
    %(errors)s, 
    %(errorRate)s)
RETURNING id
'''

INSERT_BMA_BATCH = '''
INSERT INTO base_moving_average_batches_rel
(base_ma_id, batch_id)
VALUES(%(bmaId)s, %(batchId)s)
'''

INSERT_BMA_CONF = '''
INSERT INTO base_moving_average_conf_rel(base_ma_id, config_id)
VALUES(%(bmaId)s, %(confId)s)
'''

LATEST_BMA = '''
select 
  ma.* 
from 
  base_moving_average ma 
  inner join base_moving_average_batches_rel b on ma.id = b.base_ma_id 
  inner join base_moving_average_conf_rel c on ma.id = c.base_ma_id 
where 
  b.batch_id = %(batchId)s 
  and c.config_id = %(confId)s
order by 
  ma.end_time desc 
  fetch first 1 row only
'''


GET_CONFIGURATIONS_OF_ACTIVE_TAGS = '''
select 
  tc.id as configuration_id, 
  t1.id as master_id, 
  t2.id as error_id 
from 
  tag_configurations tc 
  inner join tags t1 on tc.master_tag_id = t1.id 
  inner join tags t2 on tc.error_tag_id = t2.id 
where 
  t1.processes = true 
  and t2.processes = true 
'''
