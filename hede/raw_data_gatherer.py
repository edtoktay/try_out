from SqlOperations import SqlOperations
import raw_data_sqls as rds
from Enums import DataStatus
import pandas as pd

ops = SqlOperations(
    db='postgres_db',
    user='postgres',
    password='p.postgres',
    host='172.16.238.10'
)


class ProcessRawData:
    def __init__(self, csv_content):
        self.df = pd.read_csv(csv_content)
        self.existing_tags = self._get_existing_tags()
        pass

    def _get_existing_tags(self):
        return ops.fetch(
            sql=rds.GET_EXISTING_TAGS,
            values={},
            columns=['id', 'tagName']
        )

    def _insert_new_tag(self, tag_name):
        return ops.fetch(
            sql=rds.INSERT_NEW_TAG,
            values={
                'tagName': tag_name
            },
            columns=['id']
        )['id']

    def _get_current_partition(self, batch_name):
        return ops.fetch(
            sql=rds.GET_LATEST_BATCH_PARTITION,
            values={'batchName': batch_name},
            columns=['id', 'batchName', 'product', 'startTime', 'endTime', 'partition'])

    def _exists_timestamp_value(self, batch_name, tag_name, timestamp, value):
        return ops.fetch(
            sql=rds.EXISTS_TIMESTAMP_VALUE,
            values={
                'batchName': batch_name,
                'tagName': tag_name,
                'timestamp': timestamp,
                'value': value
            },
            columns=['isExists']
        )['isExists']

    def _finish_batch_partition(self, id, timestamp):
        ops.execute(
            sql=rds.FINISH_BATCH_PARTITION,
            values={
                'id': id,
                'endTime': timestamp
            }
        )
        pass

    def _start_new_partition(self, batch_name, product_name, start_time, previous_partition):
        partition_no = previous_partition['partition'] + 1 if previous_partition is not None else 1
        id = ops.fetch(
            sql=rds.START_NEW_BATCH_PARTITION,
            values={
                'batchName': batch_name,
                'productName': product_name,
                'startTime': start_time,
                'partition': partition_no
            },
            columns=['id']
        )['id']
        return ops.fetch(
            sql=rds.GET_BATCH_PARTITION_BY_ID,
            values={'id': id},
            columns=['id', 'batchName', 'product', 'startTime', 'endTime', 'partition'])

    def _insert_new_raw_data(self, batch_name, product, tag_name, timestamp, value, current_batch_id):
        inserted_id = ops.fetch(
            sql=rds.INSERT_NEW_RAW_DATA,
            values={
                'batchName': batch_name,
                'productName': product,
                'tagName': tag_name,
                'timestamp': timestamp,
                'value': value,
                'status': DataStatus.NEW.value if value > 0 else DataStatus.IGNORE.value
            },
            columns=['id']
        )['id']
        if current_batch_id is not None:
            ops.execute(
                sql=rds.INSERT_RAW_BATCH_REL,
                values={
                    'rawId': inserted_id,
                    'batchId': current_batch_id
                }
            )
        tag_id = None
        for t in self.existing_tags:
            if t['tagName'] == tag_name:
                tag_id = t['id']
                break
        if tag_id is None:
            tag_id = self._insert_new_tag(tag_name=tag_name)
            self.existing_tags.append({'id': tag_id, 'tagName': tag_name})
        ops.execute(
            sql=rds.INSERT_RAW_TAG_REL,
            values={
                'rawId': inserted_id,
                'tagId': tag_id
            }
        )
        pass

    def _process_batch_data(self, batch_df):
        batches_to_process = set()
        requires_new_partition = False
        for index, row in batch_df.iterrows():
            batch_name = row['BATCH_ID']
            product = row['PRODUCT_ID']
            tag_name = row['TAG_ID']
            timestamp = row['TIMESTAMP']
            value = row['VALUE']
            current_partition = self._get_current_partition(
                batch_name=batch_name)
            if not self._exists_timestamp_value(
                    batch_name=batch_name,
                    tag_name=tag_name,
                    timestamp=timestamp,
                    value=value):
                if value == 0 \
                    and current_partition is not None \
                        and current_partition['endTime'] is None:
                    self._finish_batch_partition(
                        id=current_partition['id'], timestamp=timestamp)
                    requires_new_partition = True
                elif value > 0 \
                        and (requires_new_partition or current_partition is None):
                    current_partition = self._start_new_partition(
                        batch_name=batch_name,
                        product_name=product,
                        start_time=timestamp,
                        previous_partition=current_partition)
                    requires_new_partition = False
                current_partition_id = current_partition['id'] if current_partition is not None else None
                self._insert_new_raw_data(
                    batch_name=batch_name,
                    product=product,
                    tag_name=tag_name,
                    timestamp=timestamp,
                    value=value,
                    current_batch_id=current_partition_id
                )
                if current_partition_id is not None:
                    batches_to_process.add(current_partition['id'])
        return batches_to_process

    def process_csv_file(self):
        csv_bathes = self.df['BATCH_ID'].unique()
        batch_groupped_dataframe = self.df.groupby('BATCH_ID')
        batches_to_process = set()
        for csv_batch in csv_bathes:
            batches_to_process |= self._process_batch_data(
                batch_df=batch_groupped_dataframe.get_group(csv_batch))
        return batches_to_process
