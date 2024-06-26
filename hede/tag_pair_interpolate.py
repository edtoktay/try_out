from Enums import DataType, DataStatus
from SqlOperations import SqlOperations
import pandas as pd
import numpy as np
import interpolation_data_sqls as ids

ops = SqlOperations(
    db='postgres_db',
    user='postgres',
    password='p.postgres',
    host='172.16.238.10'
)


class MasterErrorTagsInterpolation:
    def __init__(self, batch_id, master_tag_id, error_tag_id):
        self.batch_id = batch_id
        self.master_tag_id = master_tag_id
        self.error_tag_id = error_tag_id
        self.master_data_to_process = self._get_raw_tag_data(
            tag_id=master_tag_id)
        self.master_timestamps = set([d['timestamp']
                                     for d in self.master_data_to_process])
        self.error_data_to_process = self._get_raw_tag_data(
            tag_id=error_tag_id)
        self.error_timestamps = set([d['timestamp']
                                     for d in self.error_data_to_process])
        pass

    def _get_raw_tag_data(self, tag_id):
        raws = ops.fetch(
            sql=ids.GET_TAGS_NEW_DATA,
            values={
                'tagId': tag_id,
                'batchId': self.batch_id
            },
            columns=['timestamp', 'value']
        )
        for r in raws:
            self._update_raw_status(
                tag_id=tag_id,
                timestamp=r['timestamp'],
                status=DataStatus.IN_PROGRESS)
        return raws

    def _update_raw_status(self, status, timestamp, tag_id):
        ops.execute(
            sql=ids.UPDATE_RAW_STATUS,
            values={
                'tagId': tag_id,
                'batchId': self.batch_id,
                'timestamp': timestamp,
                'status': status.value
            }
        )
        pass

    def _get_latest_original_record_timestamp(self, tag_id):
        ts = ops.fetch(
            sql=ids.GET_LATEST_ORIGINAL_DATA_TIME,
            values={
                'batchId':  self.batch_id,
                'tagId': tag_id
            },
            columns=['timestamp']
        )
        return ts['timestamp'] if ts is not None else None

    def _get_sum_of_tag_before_time(self, tag_id, timestamp):
        return ops.fetch(
            sql=ids.GET_THE_SUM_BEFORE,
            values={
                'batchId': self.batch_id,
                'tagId': tag_id,
                'timestamp': timestamp
            },
            columns=['sum']
        )['sum']

    def _get_previously_interpolated_data_till(self, tag_id, timestamp):
        x = ops.fetch(
            sql=ids.GET_INTERPOLATED_DATA_FROM_GIVEN_TIME,
            values={
                'batchId': self.batch_id,
                'tagId': tag_id,
                'timestamp': timestamp
            },
            columns=['id', 'timestamp', 'value', 'type']
        )
        return x if x is not None else []

    def _insert_interpolated_batch(self, interpolated_id):
        ops.execute(
            sql=ids.INSERT_INTERPOLATED_BATCH,
            values={
                'interpolatedId': interpolated_id,
                'batchId': self.batch_id
            }
        )
        pass

    def _insert_interpolated_tag(self, interpolated_id, tag_id):
        ops.execute(
            sql=ids.INSERT_INTERPOLATED_TAG,
            values={
                'interpolatedId': interpolated_id,
                'tagId': tag_id
            }
        )
        pass

    def _insert_interpolated_data(self, timestamp, value, type, tag_id):
        inserted_id = ops.fetch(
            sql=ids.INSERT_INTERPOLATED_DATA,
            values={
                'timestamp': timestamp,
                'value': value,
                'type': type.value
            },
            columns=['id']
        )['id']
        self._insert_interpolated_batch(interpolated_id=inserted_id)
        self._insert_interpolated_tag(
            interpolated_id=inserted_id, tag_id=tag_id)
        return inserted_id

    def _update_interpolated_data(self, value, type, timestamp, tag_id):
        ops.execute(
            sql=ids.UPDATE_INTERPOLATED_DATA,
            values={
                'id': id,
                'value': value,
                'timestamp': timestamp,
                'batchId': self.batch_id,
                'tagId': tag_id,
                'type': type.value
            }
        )
        pass

    def _has_lagged_record(self, tag_id, timestamp):
        return ops.fetch(
            sql=ids.IS_INTERPOLATED_DATA_EXISTS_FOR_TIMESTAMP,
            values={
                'batchId': self.batch_id,
                'tagId': tag_id,
                'timestamp': timestamp
            },
            columns=['exists']
        )['exists']

    def _is_interpolated_exits(self, tag_id, timestamp):
        return ops.fetch(
            sql=ids.IS_INTERPOLATED_EXISTS,
            values={
                'batchId': self.batch_id,
                'tagId': tag_id,
                'timestamp': timestamp
            },
            columns=['exists']
        )['exists']

    def _clear_duplicates(self, data_frame, field_name, sort_values_list=None, is_keep_last=True):
        df = data_frame.copy()
        fields_to_evaluate = df[field_name]
        duplicated_sorted_values = df[
            fields_to_evaluate.isin(
                fields_to_evaluate[fields_to_evaluate.duplicated()])
        ].sort_values(
            sort_values_list
            if sort_values_list is not None
            else [field_name])
        if duplicated_sorted_values.empty:
            return data_frame
        duplicated_sorted_values.drop_duplicates(
            subset=field_name, keep='last' if is_keep_last else 'first', inplace=True)
        first_duplicate_removed = pd.merge(
            df,
            duplicated_sorted_values,
            on=sort_values_list
            if sort_values_list is not None
            else [field_name],
            how='outer',
            indicator=True
        ).query("_merge != 'both'").drop("_merge", axis=1).reset_index(drop=True)
        return self._clear_duplicates(data_frame=first_duplicate_removed,
                                      field_name=field_name,
                                      sort_values_list=sort_values_list,
                                      is_keep_last=is_keep_last)

    def _data_cleaning(self, data_frame, tag_id):
        # Clear duplicated timestamps for data
        data_frame = self._clear_duplicates(
            data_frame=data_frame,
            field_name='timestamp',
            sort_values_list=['timestamp', 'value'],
            is_keep_last=False
        )
        # Clear duplicated values for data
        data_frame = self._clear_duplicates(
            data_frame=data_frame,
            field_name='value',
            sort_values_list=['timestamp', 'value']
        )
        latest_original_timestamp = self._get_latest_original_record_timestamp(
            tag_id=tag_id
        )
        # if latest original is after any current data.
        # New data contains already processed data so discard that data and mark as already processed
        if latest_original_timestamp is not None:
            data_frame = data_frame.drop(
                data_frame[data_frame.timestamp <= latest_original_timestamp].index, inplace=True)
            data_frame.reset_index()
        return data_frame

    def _data_enriching(self, data_dictionary, tag_id, timestamp):
        sum_before_timestamp = self._get_sum_of_tag_before_time(
            tag_id=tag_id,
            timestamp=timestamp)
        previously_interpolated = self._get_previously_interpolated_data_till(
            tag_id=tag_id,
            timestamp=timestamp
        )
        for historic_data in previously_interpolated:
            current_val = historic_data['value']
            historic_data['value'] = historic_data['value'] + \
                sum_before_timestamp
            sum_before_timestamp += current_val
        data_dictionary = previously_interpolated + data_dictionary
        return data_dictionary, \
            sum_before_timestamp \
            if sum_before_timestamp is not None else 0

    def _data_validation(self):
        pass

    def _create_data_frame(self, data_set, tag_id, timestamp):
        data_set, sum_before_timestamp = self._data_enriching(
            data_dictionary=data_set,
            tag_id=tag_id,
            timestamp=timestamp)
        return pd.DataFrame(data_set), sum_before_timestamp

    # validate that both data set started from same time
    def _validate_same_starting_time(self, df1, df2):
        if df1.iloc[0]['timestamp'] != df2.iloc[0]['timestamp']:
            # both data sets not started at the same time add 0 to initiate for missing one
            if df1.iloc[0]['timestamp'] < df2.iloc[0]['timestamp']:
                df2.loc[-1] = [df1.iloc[0]['timestamp'], 0]
                df2.index = df2.index + 1
                df2.sort_index(inplace=True)
            else:
                df1.loc[-1] = [df2.iloc[0]['timestamp'], 0]
                df1.index = df1.index + 1
                df1.sort_index(inplace=True)
        pass

    # validate both data sets finished at the same time
    def _validate_same_end_time(self, df1, df2):
        if df1.iloc[len(df1)-1]['timestamp'] != df2.iloc[len(df2)-1]['timestamp']:
            # both data sets are not finished at the same time add latest time with the lastest value
            if df1.iloc[0]['timestamp'] < df2.iloc[0]['timestamp']:
                df1.loc[len(df1)] = [df2.iloc[len(df2)-1]['timestamp'],
                                     df1.iloc[len(df1)-1]['value']]
                df1.sort_index(inplace=True)
            else:
                df2.loc[len(df2)] = [df1.iloc[len(df1)-1]['timestamp'],
                                     df2.iloc[len(df2)-1]['value']]
                df2.sort_index(inplace=True)
        pass

    def _interpolate_data(self, df):
        timeSerie = pd.Series(df['value'].tolist(),
                              index=df['timestamp'].tolist())
        timeSerie[timeSerie == -1] = np.nan
        timeSerie = timeSerie.resample('min')
        timeSerie = timeSerie.interpolate(method='time')
        return pd.DataFrame({'timestamp': timeSerie.index, 'value': timeSerie.values})

    def process_tags(self):
        master_tag_latest_original_timestamp = self._get_latest_original_record_timestamp(
            tag_id=self.master_tag_id
        )
        error_tag_latest_original_timestamp = self._get_latest_original_record_timestamp(
            tag_id=self.error_tag_id
        )
        master_tag_latest_original_timestamp = master_tag_latest_original_timestamp \
            if master_tag_latest_original_timestamp is not None \
            else self.master_data_to_process[0]['timestamp']
        error_tag_latest_original_timestamp = error_tag_latest_original_timestamp \
            if error_tag_latest_original_timestamp is not None \
            else self.error_data_to_process[0]['timestamp']
        # use the earliest timestamp as starting point
        interpolaration_starting_point = master_tag_latest_original_timestamp \
            if master_tag_latest_original_timestamp < error_tag_latest_original_timestamp \
            else error_tag_latest_original_timestamp
        # create data frames for enriched data
        master_tag_dataframe, master_tag_sum = self._create_data_frame(
            data_set=self.master_data_to_process,
            tag_id=self.master_tag_id,
            timestamp=interpolaration_starting_point)
        error_tag_dataframe, error_tag_sum = self._create_data_frame(
            data_set=self.error_data_to_process,
            tag_id=self.error_tag_id,
            timestamp=interpolaration_starting_point)
        # validate that both data set started from same time
        self._validate_same_starting_time(
            df1=master_tag_dataframe,
            df2=error_tag_dataframe)
        # validate both data sets not finished at the same time
        self._validate_same_end_time(
            df1=master_tag_dataframe,
            df2=error_tag_dataframe
        )
        # cleanize data frames
        master_tag_dataframe = self._data_cleaning(
            data_frame=master_tag_dataframe,
            tag_id=self.master_tag_id)
        error_tag_dataframe = self._data_cleaning(
            data_frame=error_tag_dataframe,
            tag_id=self.error_tag_id)
        # interpolate frames
        master_tag_dataframe = self._interpolate_data(df=master_tag_dataframe)
        master_tag_dataframe.value = master_tag_dataframe.value.round()
        error_tag_dataframe = self._interpolate_data(df=error_tag_dataframe)
        error_tag_dataframe.value = error_tag_dataframe.value.round()
        merged_data_frame = pd.merge(
            master_tag_dataframe, error_tag_dataframe, on='timestamp', how='left')
        merged_data_frame = merged_data_frame.set_index('timestamp')
        # Fix first row data
        first_row = merged_data_frame.head(1)
        merged_data_frame = merged_data_frame.diff().fillna(0)
        value_x = first_row.iloc[0]['value_x'] - master_tag_sum
        value_y = first_row.iloc[0]['value_y'] - error_tag_sum
        merged_data_frame.loc[first_row.index, 'value_x'] = value_x
        merged_data_frame.loc[first_row.index, 'value_y'] = value_y
        for index, row in merged_data_frame.iterrows():
            # Save master tag data
            master_type = DataType.ORIGINAL if index in self.master_timestamps else DataType.INTERPOLATED
            if self._is_interpolated_exits(
                    tag_id=self.master_tag_id,
                    timestamp=index):
                self._update_interpolated_data(
                    tag_id=self.master_tag_id,
                    timestamp=index,
                    value=row['value_x'],
                    type=master_type)
            else:
                self._insert_interpolated_data(
                    timestamp=index,
                    tag_id=self.master_tag_id,
                    type=master_type,
                    value=row['value_x']
                )
            if master_type == DataType.ORIGINAL:
                self._update_raw_status(
                    tag_id=self.master_tag_id,
                    timestamp=index,
                    status=DataStatus.COMPLETED)
            error_type = DataType.ORIGINAL if index in self.error_timestamps else DataType.INTERPOLATED
            if self._is_interpolated_exits(
                    tag_id=self.error_tag_id,
                    timestamp=index):
                self._update_interpolated_data(
                    tag_id=self.error_tag_id,
                    timestamp=index,
                    value=row['value_y'],
                    type=error_type)
            else:
                self._insert_interpolated_data(
                    timestamp=index,
                    tag_id=self.error_tag_id,
                    type=error_type,
                    value=row['value_y']
                )
            if error_type == DataType.ORIGINAL:
                self._update_raw_status(
                    tag_id=self.error_tag_id,
                    timestamp=index,
                    status=DataStatus.COMPLETED)
        return first_row.index