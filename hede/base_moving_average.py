from SqlOperations import SqlOperations
import bma_sqls as bs
import pandas as pd
import numpy as np
import math

ops = SqlOperations(
    db='postgres_db',
    user='postgres',
    password='p.postgres',
    host='172.16.238.10'
)


class BMA:
    def __init__(self, batch_id, timestamp, boxing_size):
        self.batch_id = batch_id
        self.timestamp = timestamp
        self.boxing_size = boxing_size
        self.tag_configs = self._get_active_configurations()
        if self.tag_configs is None or len(self.tag_configs) == 0:
            raise
        self.boxed_moving_average = pd.DataFrame(
            columns=['Timestamp', 'Success', 'Error', 'Error_Percentage']
        )
        self.production_total = 0
        self.error_total = 0
        pass

    def _get_active_configurations(self):
        return ops.fetch(
            sql=bs.GET_CONFIGURATIONS_OF_ACTIVE_TAGS,
            values=[],
            columns=['id', 'masterTagId', 'errorTagId']
        )

    def _get_latest_moving_average(self, config_id):
        return ops.fetch(
            sql=bs.LATEST_BMA,
            values={
                'batchId': self.batch_id,
                'confId': config_id
            },
            columns=['id', 'startTime', 'endTime', 'totalStart', 'totalEnd',
                     'errorStart', 'errorEnd', 'size', 'errors', 'errorRate']
        )

    def _get_interpolated_data(self, tag_id, timestamp):
        return ops.fetch(
            sql=bs.GET_INTERPOLATED_VALUES_OF_TAG,
            values={
                'batchId': self.batch_id,
                'tagId': tag_id,
                'timestamp': timestamp
            },
            columns=['timestamp', 'value']
        )

    def _get_latest_boxed_data_values(self, tag_id, start_time, end_time):
        return ops.fetch(
            sql=bs.GET_LATEST_CALCULATED_INTERPOLATION,
            values={
                'batchId': self.batch_id,
                'tagId': tag_id,
                'startTime': start_time,
                'endTime': end_time
            },
            columns=['timestamp', 'value']
        )

    def _insert_bma_batch(self, bma_id):
        ops.execute(
            sql=bs.INSERT_BMA_BATCH,
            values={
                'bmaId': bma_id,
                'batchId': self.batch_id
            }
        )
        pass

    def _insert_bma_config(self, bma_id, conf_id):
        ops.execute(
            sql=bs.INSERT_BMA_CONF,
            values={
                'bmaId': bma_id,
                'confId': conf_id
            }
        )
        pass

    def _insert_bma(self, start_time, end_time, total_start, total_end, error_start, error_end, size, error_current_total, error_rate, conf_id):
        idx = ops.fetch(
            sql=bs.INSERT_BASE_MOVING_AVG,
            values={
                'startTime': start_time,
                'endTime': end_time,
                'totalStart': total_start,
                'totalEnd': total_end,
                'errorStart': error_start,
                'errorEnd': error_end,
                'size': size,
                'errors': error_current_total,
                'errorRate': error_rate
            },
            columns=['id']
        )['id']
        self._insert_bma_batch(bma_id=idx)
        self._insert_bma_config(bma_id=idx, conf_id=conf_id)
        pass

    def _get_box_frame(self, conf_id, master_tag_id, error_tag_id):
        previous_frame = self._get_latest_moving_average(config_id=conf_id)
        if previous_frame is not None:
            # Total production/error is previous calculations start values because latest one will be re-calculated
            self.production_total = previous_frame['totalStart']
            self.error_total = previous_frame['errorStart']
            previous_master_boxing_values = self._get_latest_boxed_data_values(
                tag_id=master_tag_id,
                start_time=previous_frame['startTime'],
                end_time=previous_frame['endTime'])
            previous_error_boxing_values = self._get_latest_boxed_data_values(
                tag_id=error_tag_id,
                start_time=previous_frame['startTime'],
                end_time=previous_frame['endTime'])
            merged_previous_data = pd.DataFrame(previous_master_boxing_values).merge(
                pd.DataFrame(previous_error_boxing_values), on='timestamp', how='left'
            )
            print(merged_previous_data)
            for i, r in merged_previous_data.iterrows():
                timestamp = r['timestamp']
                produced = r['value_x']
                error = r['value_y']
                self.boxed_moving_average.loc[len(self.boxed_moving_average)] = [
                    timestamp, produced, error, error/produced * 100]
        pass

    def _interpolate_bma_box(self):
        print(self.boxed_moving_average.head(50))
        production = self.boxed_moving_average['Success'].sum()
        error = self.boxed_moving_average['Error'].sum()
        if production >= self.boxing_size:
            idx = False
            diff = 0
            approximate_errors_for_diff = 0
            for i, r in self.boxed_moving_average.iterrows():
                row_production = r['Success']
                row_error = r['Error']
                production -= int(row_production)
                error -= int(row_error)
                if production < self.boxing_size:
                    diff = self.boxing_size - production
                    approximate_errors_for_diff = math.ceil(
                        diff * row_error / row_production)
                    self.boxed_moving_average.loc[i, 'Success'] = diff
                    self.boxed_moving_average.loc[i,
                                                  'Error'] = approximate_errors_for_diff
                    self.boxed_moving_average.loc[i,
                                                  'Error_Percentage'] = approximate_errors_for_diff / diff * 100
                    print(self.boxed_moving_average.head(50))
                else:
                    idx = True
                break
            if idx:
                self.boxed_moving_average = self.boxed_moving_average.iloc[1:, :]
                self.boxed_moving_average.reset_index(drop=True, inplace=True)
                self._interpolate_bma_box()
        pass

    def _box_data(self, configuration):
        self._get_box_frame(
            conf_id=configuration['id'],
            master_tag_id=configuration['masterTagId'],
            error_tag_id=configuration['errorTagId'])
        if (len(self.boxed_moving_average)) > 0:
            self._interpolate_bma_box()
        master_interpolated_data = pd.DataFrame(self._get_interpolated_data(
            tag_id=configuration['masterTagId'],
            timestamp=self.timestamp))
        print(master_interpolated_data)
        error_interpolated_data = pd.DataFrame(self._get_interpolated_data(
            tag_id=configuration['errorTagId'],
            timestamp=self.timestamp))
        print(error_interpolated_data)
        merged_interpolated = master_interpolated_data.merge(
            error_interpolated_data, on='timestamp', how='left'
        )
        print(merged_interpolated.head(150))
        for i, r in merged_interpolated.iterrows():
            timestamp = r['timestamp']
            produced = r['value_x']
            error = r['value_y']
            self.production_total += produced
            self.error_total += error
            self.boxed_moving_average.loc[len(self.boxed_moving_average)] = [
                timestamp, produced, error, error/produced * 100]
            self._interpolate_bma_box()
            size = self.boxed_moving_average['Success'].sum()
            current_error_total = self.boxed_moving_average['Error'].sum()
            self._insert_bma(
                start_time=self.boxed_moving_average.iloc[0]['Timestamp'],
                end_time=self.boxed_moving_average.iloc[len(
                    self.boxed_moving_average) - 1]['Timestamp'],
                total_start=self.production_total - size,
                total_end=self.production_total,
                error_start=self.error_total - current_error_total,
                error_end=self.error_total,
                size=size,
                error_current_total=current_error_total,
                error_rate=current_error_total / size * 100,
                conf_id=configuration['id']
            )
        pass

    def process_base_moving_averages(self):
        self._box_data(self.tag_configs)
