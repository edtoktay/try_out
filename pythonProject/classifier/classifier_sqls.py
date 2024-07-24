FIRST_THREE_PARTITIONS = '''
select 
id, 
start_time, 
end_time 
from tag_reading_partitions t
order by t.id desc 
fetch first 3 row only
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