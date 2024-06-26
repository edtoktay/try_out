GET_TAGS_NEW_DATA = '''
select 
  r."TIMESTAMP", 
  r."VALUE" 
from 
  raw_tag_read r 
  inner join raw_read_batch_rel b on r.id = b.raw_id 
  inner join raw_read_tag_rel t on r.id = t.raw_id 
where 
  t.tag_id = %(tagId)s
  and b.batch_id = %(batchId)s 
  and r."STATUS" = 'NEW' 
order by 
  r."TIMESTAMP" asc
'''

UPDATE_RAW_STATUS = '''
update 
  raw_tag_read 
set 
  "STATUS" = %(status)s 
where 
  id in (
    select 
      r.id 
    from 
      raw_tag_read r 
      inner join raw_read_batch_rel b on r.id = b.raw_id 
      inner join raw_read_tag_rel t on r.id = t.raw_id 
    where 
      r."TIMESTAMP" = %(timestamp)s 
      and b.batch_id = %(batchId)s 
      and t.tag_id = %(tagId)s
  )
'''

UPDATE_INTERPOLATED_DATA = '''
update 
  raw_interpolated 
set 
  "VALUE" = %(value)s, 
  "TYPE" = %(type)s 
where 
  id = (
    select 
      id 
    from 
      raw_interpolated r 
      inner join raw_interpolated_batches_rel b on r.id = b.raw_interpolate_id 
      inner join raw_interpolated_tags_rel t on r.id = t.raw_interpolate_id 
    where 
      r."TIMESTAMP" = %(timestamp)s 
      and b.batch_id = %(batchId)s 
      and t.tag_id = %(tagId)s
  )
'''

INSERT_INTERPOLATED_DATA = '''
INSERT INTO raw_interpolated ("TIMESTAMP", "VALUE", "TYPE") 
VALUES (%(timestamp)s, %(value)s, %(type)s)
RETURNING id
'''

INSERT_INTERPOLATED_BATCH = '''
INSERT INTO raw_interpolated_batches_rel(raw_interpolate_id, batch_id)
VALUES(%(interpolatedId)s, %(batchId)s)
'''

INSERT_INTERPOLATED_TAG = '''
INSERT INTO raw_interpolated_tags_rel(raw_interpolate_id, tag_id)
VALUES(%(interpolatedId)s, %(tagId)s)
'''

GET_LATEST_ORIGINAL_DATA_TIME = '''
select 
    r."TIMESTAMP" 
from raw_interpolated r
inner join raw_interpolated_batches_rel rb
on r.id = rb.raw_interpolate_id
inner join raw_interpolated_tags_rel rt
on r.id = rt.raw_interpolate_id
where
	rb.batch_id = %(batchId)s
	and rt.tag_id = %(tagId)s
    and r."TYPE" = 'ORIGINAL'
order by r."TIMESTAMP" asc 
fetch first 1 row only
'''

GET_THE_SUM_BEFORE = '''
select
	sum(r."VALUE")
from
	raw_interpolated r
inner join raw_interpolated_batches_rel rb
on r.id = rb.raw_interpolate_id
inner join raw_interpolated_tags_rel rt
on r.id = rt.raw_interpolate_id
where
	rb.batch_id = %(batchId)s
	and rt.tag_id = %(tagId)s
    and r."TIMESTAMP" > %(timestamp)s
'''

GET_INTERPOLATED_DATA_FROM_GIVEN_TIME = '''
select 
    r."TIMESTAMP", 
    r."VALUE"
from raw_interpolated r
inner join raw_interpolated_batches_rel rb
on r.id = rb.raw_interpolate_id
inner join raw_interpolated_tags_rel rt
on r.id = rt.raw_interpolate_id
where
	rb.batch_id = %(batchId)s
	and rt.tag_id = %(tagId)s
    and r."TIMESTAMP" >= %(timestamp)s
order by r."TIMESTAMP" asc 
'''

IS_INTERPOLATED_DATA_EXISTS_FOR_TIMESTAMP = '''
select
    exists (
        select
            1
        from raw_interpolated r
        inner join raw_interpolated_batches_rel rb
        on r.id = rb.raw_interpolate_id
        inner join raw_interpolated_tags_rel rt
        on r.id = rt.raw_interpolate_id
        where
            rb.batch_id = %(batchId)s
            and rt.tag_id = %(tagId)s
            and r."TYPE" = 'INTERPOLATED'
            and r."TIMESTAMP" > %(timestamp)s
    )
'''

IS_INTERPOLATED_EXISTS = '''
select
    exists (
        select
            1
        from raw_interpolated r
        inner join raw_interpolated_batches_rel rb
        on r.id = rb.raw_interpolate_id
        inner join raw_interpolated_tags_rel rt
        on r.id = rt.raw_interpolate_id
        where
            rb.batch_id = %(batchId)s
            and rt.tag_id = %(tagId)s
            and r."TIMESTAMP" = %(timestamp)s
    )
'''
