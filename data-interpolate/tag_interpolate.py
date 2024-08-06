import pandas as pd
import numpy as np
from data_cleaner import DataCleaner
from interpolate_data_operations import DatabaseOps


class TagInterpolate:
    def __init__(self, data):
        self.data = DataCleaner(data).clean_data(check_sort_order=True)
        db_ops = DatabaseOps()
        self.tag = db_ops.get_tag(self.data['tag_id'].iloc[0])
        self.partition_id = self.data['partition_id'].iloc[0]
        self.partition = db_ops.get_partition(self.partition_id)

    def interpolate(self) -> pd.DataFrame:
        if self.data.iloc[0]['timestamp'] > self.partition['start_time']:
            self.data.loc[-1] = [
                self.data.iloc[0]['tagname'],
                self.partition['start_time'],
                0,
                self.data.iloc[0]['partition_id'],
                self.data.iloc[0]['tag_id']
            ]
        time_series = pd.Series(
            self.data['value'].tolist(),
            index=self.data['timestamp'].tolist())
        time_series[time_series == -1] = np.nan
        time_series = time_series.resample('min')
        time_series = time_series.interpolate(method='time')
        df = pd.DataFrame({'timestamp': time_series.index,
                          'value': time_series.values})
        df['tag_name'] = self.tag['tag_name']
        df['tag_id'] = self.tag['id']
        df['partition_id'] = self.partition_id
        return df
