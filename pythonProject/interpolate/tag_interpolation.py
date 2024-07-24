import numpy as np
import pandas as pd
from commons.file_operations import FileOps


import pandas as pd
import numpy as np


class TagInterpolation:
    """
    A class for performing time series interpolation on tag data.

    Args:
        csv_content (str): The path to the CSV file containing the tag data.
        bucket_name (str): The name of the bucket to store the interpolated data.

    Attributes:
        data (pandas.DataFrame): The tag data read from the CSV file.
        partition_id (int): The partition ID of the tag data.
        tag_id (int): The tag ID of the tag data.
        tag_name (str): The name of the tag.
        file_ops (FileOps): An instance of the FileOps class for file operations.

    Methods:
        interpolate: Performs time series interpolation on the tag data.

    """

    def __init__(self, csv_content, bucket_name) -> None:
        """
        Initializes a new instance of the TagInterpolation class.

        Args:
            csv_content (str): The path to the CSV file containing the tag data.
            bucket_name (str): The name of the bucket to store the interpolated data.

        """
        self.data = pd.read_csv(csv_content)
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        self.data['value'] = pd.to_numeric(
            self.data['value'], downcast='integer', errors='coerce')
        self.data = self.data.sort_values(
            by='timestamp').reset_index(drop=True)
        self.partition_id = self.data['partition_id'][0]
        self.tag_id = self.data['tag_id'][0]
        self.tag_name = self.data['tag_name'][0]
        self.file_ops = FileOps(bucket_name)
        pass

    def __time_series_interpolation(self):
        """
        Performs time series interpolation on the tag data.

        Returns:
            pandas.DataFrame: The interpolated tag data.

        """
        time_series = pd.Series(
            self.data['value'].tolist(),
            index=self.data['timestamp'])
        time_series[time_series == -1] = np.nan
        time_series = time_series.resample('min')
        time_series = time_series.interpolate(method='time')
        return pd.DataFrame({
            'partition_id': self.partition_id,
            'tag_id': self.tag_id,
            'tag_name': self.tag_name,
            'timestamp': time_series.index,
            'value': time_series.values
        })

    def interpolate(self):
        """
        Performs time series interpolation on the tag data and persists the interpolated data.

        """
        self.data = self.__time_series_interpolation()
        self.file_ops.persist_data(
            self.data,
            self.tag_name + '/' + self.partition_id + '/' + self.tag_id + '.csv')
