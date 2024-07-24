import pandas as pd
import logging
from data_cleaner import DataCleaner
from classifier_data_operations import DatabaseOps

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DataClassifier:
    """
    A class for classifying data based on tags.

    Args:
        csv_content (str): The content of the CSV file.

    Attributes:
        tag_partitioned_data (dict): A dictionary containing the partitioned tag data.
        db_ops (DatabaseOps): An instance of the DatabaseOps class.
        partitions (list): A list of partitions.
        tags (list): A list of tags.

    Methods:
        __find_tag: Private method to find a tag by name.
        __find_control_tag: Private method to find the control tag.
        __find_batch_tag: Private method to find the batch tag.
        __find_lot_size_tag: Private method to find the lot size tag.
        __find_active_partition: Private method to find the active partition.
        __check_dataframe_incremental: Private method to check if the 'value' column is incremental.
        __handle_batch_tag: Private method to handle the batch tag.
        __handle_lot_size_tag: Private method to handle the lot size tag.
        __get_appropriate_partition: Private method to get the appropriate partition for a tag time.
        __requires_new_partition: Private method to check if a new partition is required.
        __handle_control_tag: Private method to handle the control tag.
        classify_data: Method to classify the data.

    """

    def __init__(self, csv_content):
        data_cleaner = DataCleaner(csv_content=csv_content)
        self.tag_partitioned_data = data_cleaner.get_tag_data()
        self.db_ops = DatabaseOps()
        self.partitions = self.db_ops.get_partitions()
        if type(self.partitions) is not list:
            self.partitions = [self.partitions]
        self.tags = self.db_ops.get_tags()
        pass

    def __find_tag(self, tag_name):
        """
        Private method to find a tag by name.

        Args:
            tag_name (str): The name of the tag.

        Returns:
            dict: The tag information.

        """
        for tag in self.tags:
            if tag['tag_name'] == tag_name:
                return tag
        self.tags = self.db_ops.insert_tag(tag_name)
        return self.__find_tag(tag_name)

    def __find_control_tag(self):
        """
        Private method to find the control tag.

        Returns:
            dict or None: The control tag information, or None if not found.

        """
        for tag in self.tags:
            if tag['is_control_tag']:
                return tag
        return None

    def __find_batch_tag(self):
        """
        Private method to find the batch tag.

        Returns:
            dict or None: The batch tag information, or None if not found.

        """
        for tag in self.tags:
            if tag['is_batch_name']:
                return tag
        return None

    def __find_lot_size_tag(self):
        """
        Private method to find the lot size tag.

        Returns:
            dict or None: The lot size tag information, or None if not found.

        """
        for tag in self.tags:
            if tag['is_lot_size']:
                return tag
        return None

    def __find_active_partition(self):
        """
        Private method to find the active partition.

        Returns:
            dict or None: The active partition information, or None if not found.

        """
        for partition in self.partitions:
            if partition['end_time'] is None:
                return partition
        return None

    @staticmethod
    def __check_dataframe_incremental(data_frame):
        """
        Private method to check if the 'value' column is incremental.

        Args:
            data_frame (pd.DataFrame): The DataFrame to check.

        Returns:
            pd.DataFrame: The updated DataFrame with non-incremental values removed.

        """
        # Check if the 'value' column is incremental
        is_incremental = data_frame['value'].is_monotonic_increasing
        if not is_incremental:
            # Find the columns that cause non-incremental values
            difference_series = data_frame['value'].diff().fillna(1).le(0)
            non_incremental_columns = difference_series[difference_series]
            for index in non_incremental_columns.index:
                data_frame = data_frame.drop(index)

            # Delete the columns that cause non-incremental values
            data_frame.reset_index(inplace=True)
        return data_frame

    def __handle_batch_tag(self, response):
        """
        Private method to handle the batch tag.

        Args:
            response (dict): The response dictionary.

        """
        batch_tag = self.__find_batch_tag()
        if batch_tag:
            batch_tag_data = self.tag_partitioned_data[batch_tag['tag_name']]
            self.tag_partitioned_data.pop(batch_tag['tag_name'])
            response[batch_tag['tag_name']] = batch_tag_data
        pass

    def __handle_lot_size_tag(self, response):
        """
        Private method to handle the lot size tag.

        Args:
            response (dict): The response dictionary.

        """
        lot_size_tag = self.__find_lot_size_tag()
        if lot_size_tag:
            lot_size_tag_data = self.tag_partitioned_data[lot_size_tag['tag_name']]
            self.tag_partitioned_data.pop(lot_size_tag['tag_name'])
            response[lot_size_tag['tag_name']] = lot_size_tag_data
        pass

    def __get_appropriate_partition(self, tag_time):
        """
        Private method to get the appropriate partition for a tag time.

        Args:
            tag_time (datetime): The timestamp of the tag.

        Returns:
            dict: The appropriate partition information.

        """
        for partition in self.partitions:
            if partition['start_time'] <= tag_time <= partition['end_time'] \
                    if partition['end_time'] else True:
                return partition
        return self.partitions[-1]

    def __requires_new_partition(self, tag_time):
        """
        Private method to check if a new partition is required.

        Args:
            tag_time (datetime): The timestamp of the tag.

        Returns:
            bool: True if a new partition is required, False otherwise.

        """
        active_partition = self.__find_active_partition()
        return active_partition['end_time'] and active_partition['end_time'] < tag_time

    def __handle_control_tag(self, response):
        """
        Private method to handle the control tag.

        Args:
            response (dict): The response dictionary.

        Returns:
            pd.DataFrame: The DataFrame containing ignored tags.

        """
        control_tag = self.__find_control_tag()
        control_tag_response = pd.DataFrame(
            columns=['partition_id', 'tag_id', 'tag_name', 'timestamp', 'value'])
        ignore_tag = pd.DataFrame(
            columns=['tag_name', 'timestamp', 'value', 'status', 'reason'])
        if control_tag:
            control_tag_data = self.tag_partitioned_data[control_tag['tag_name']]
            active_partition = self.__find_active_partition()
            for index, row in control_tag_data.iterrows():
                tag_time = row['timestamp']
                tag_value = row['value']
                if tag_value == 0:
                    ignore_tag.loc[len(ignore_tag)] = [
                        control_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'IGNORE',
                        'Control tag value is 0'
                    ]
                    if active_partition:
                        logger.info(f"Ending partition with ID: {
                        active_partition['id']}")
                        self.partitions = self.db_ops.end_partition(
                            partition_id=active_partition['id'], end_time=tag_time)
                        active_partition = None
                else:
                    if active_partition is None or self.__requires_new_partition(tag_time):
                        self.db_ops.insert_partition(tag_time)
                        self.partitions = self.db_ops.get_partitions()
                    partition = self.__get_appropriate_partition(tag_time)
                    if control_tag_response.empty \
                            or control_tag_response.iloc[-1]['value'] < tag_value:
                        control_tag_response.loc[len(control_tag_response)] = [
                            partition['id'],
                            control_tag['id'],
                            control_tag['tag_name'],
                            row['timestamp'],
                            row['value']
                        ]
                    else:
                        ignore_tag.loc[len(ignore_tag)] = [
                            control_tag['tag_name'],
                            row['timestamp'],
                            row['value'],
                            'IGNORE',
                            'Control tag value is not incremental'
                        ]
        self.tag_partitioned_data.pop(control_tag['tag_name'])
        response[control_tag['tag_name']] = control_tag_response
        return ignore_tag

    def classify_data(self):
        """
        Method to classify the data.

        Returns:
            tuple: A tuple containing the response dictionary and the DataFrame of ignored tags.

        """
        response = {}
        ignore_tag = self.__handle_control_tag(response)
        self.__handle_batch_tag(response)
        self.__handle_lot_size_tag(response)
        for tag in self.tag_partitioned_data:
            current_tag = self.__find_tag(tag)
            tag_response = pd.DataFrame(
                columns=[
                    'partition_id',
                    'tag_id',
                    'tag_name',
                    'timestamp',
                    'value'])
            for index, row in self.tag_partitioned_data[tag].iterrows():
                tag_time = row['timestamp']
                tag_value = int(row['value'])
                if tag_value == 0:
                    ignore_tag.loc[len(ignore_tag)] = [
                        current_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'IGNORE',
                        'Tag value is 0'
                    ]
                else:
                    partition = self.__get_appropriate_partition(tag_time)
                    if tag_response.empty \
                            or int(tag_response.iloc[-1]['value']) < tag_value:
                        tag_response.loc[len(tag_response)] = [
                            partition['id'],
                            current_tag['id'],
                            current_tag['tag_name'],
                            row['timestamp'],
                            row['value']
                        ]
                    else:
                        ignore_tag.loc[len(ignore_tag)] = [
                            current_tag['tag_name'],
                            row['timestamp'],
                            row['value'],
                            'IGNORE',
                            'Control tag value is not incremental'
                        ]
            response[tag] = tag_response
        return response, ignore_tag
