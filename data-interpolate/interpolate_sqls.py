GET_TAG = """
select id, tag_name  from tags 
where id = %(id)s
"""

GET_PARTITION = """
select id, start_time from tag_reading_partitions
where id = %(id)s
"""
