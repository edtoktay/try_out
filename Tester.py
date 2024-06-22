from SqlOperations import SqlOperations
from ResponseMapper import ResponseMapper

ops = SqlOperations(
    db = 'postgres_db',
    user='postgres',
    password='p.postgres',
    host='172.16.238.10'
)

val = ops.query_with_return(sql='''
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
                            ''',
                            values={})

print(val)
mapper = ResponseMapper(['ConfigId', 'MasterId', 'MasterName', 'ErrorId', 'ErrorName'])
print(mapper.map(val))

print(mapper.map(None))

insert_val =  ops.query_with_return(sql='''
INSERT INTO batches(batch_id, product_id, start_time, "PARTITION") 
VALUES (%(batchId)s, %(productId)s, %(startTime)s, %(partition)s)
RETURNING id
                                    ''',
                                    values={
                                        'batchId': 'HEDE123',
                                        'productId': 'Hodo_1234',
                                        'startTime': None,
                                        'partition': 1
                                    })
print(insert_val)

insert_val =  ops.query_with_return(sql='''
INSERT INTO batches(batch_id, product_id, start_time, "PARTITION") 
VALUES (%(batchId)s, %(productId)s, %(startTime)s, %(partition)s)
RETURNING id
                                    ''',
                                    values={
                                        'batchId': 'HEDE123',
                                        'productId': 'Hodo_1234',
                                        'startTime': None,
                                        'partition': 2
                                    })
print(insert_val)