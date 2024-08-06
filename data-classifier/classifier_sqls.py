INSERT_BATCH = '''
INSERT INTO batches
(batch_id, start_time, end_time, lot_size)
VALUES(%(batchName)s, %(startTime)s, null, 0)
'''

UPDATE_BATCH_LOT_SIZE = '''
UPDATE batches
SET lot_size = %(lotSize)s
WHERE batch_id = %(batchName)s
'''

INSERT_TAG = '''
insert into tags
(tag_name,
is_batch_name,
is_lot_size,
is_control_tag)
values(
%(tag_name)s,
false,
false,
false)
'''

INSERT_PARTITION = '''
insert into tag_reading_partitions(start_time)
values(%(start_time)s)
'''

END_PARTITION = '''
update tag_reading_partitions
set end_time = %(end_time)s
where id = %(id)s
'''

GET_TAGS = '''
select
id,
tag_name,
is_batch_name,
is_lot_size,
is_control_tag
from tags
'''

LATEST_BATCH = '''
select batch_id, start_time, end_time, lot_size from batches
order by end_time desc
'''

LATEST_PARTITION = '''
select * from tag_reading_partitions
order by end_time desc limit 1
'''

FIND_CONTAINING_PARTITION = '''
SELECT * FROM tag_reading_partitions
WHERE %(timestamp)s >= start_time AND (%(timestamp)s <= end_time OR end_time IS NULL)
'''
