from tabnanny import check

import pandas as pd
import logging
from data_cleaner import DataCleaner
from classifier_data_operations import DatabaseOps
from classifier_file_operations import ClassifierFileOps

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DataClassifier:
    """
    A class for classifying data based on tags.

    Args:
        data_frame (pd.DataFrame): The input data frame.
        bucket_name (str): The name of the bucket.

    Attributes:
        data (pd.DataFrame): The input data frame.
        db_ops (DatabaseOps): An instance of the DatabaseOps class.
        file_ops (ClassifierFileOps): An instance of the ClassifierFileOps class.
        tags (list): A list of tags obtained from the database.

    Methods:
        __init__(self, data_frame: pd.DataFrame, bucket_name: str) -> None:
            Initializes the DataClassifier object.

        __find_tag(self, tag_name: str) -> dict:
            Finds a tag with the given name.

        __find_control_tag(self) -> dict:
            Finds the control tag.

        __find_batch_tag(self) -> dict:
            Finds the batch tag.

        __find_lot_size_tag(self) -> dict:
            Finds the lot size tag.

        __process_batch_information(self, batch_related_df: pd.DataFrame, ignored_values: pd.DataFrame) -> pd.DataFrame:
            Processes batch-related information.

        classify_data(self) -> None:
            Classifies the data based on tags.
    """

    def __init__(self, data_frame: pd.DataFrame, bucket_name: str) -> None:
        """
        Initializes the DataClassifier object.

        Args:
            data_frame (pd.DataFrame): The input data frame for all tags.
            bucket_name (str): The name of the s3 bucket to persist data.

        Returns:
            None
        """
        logger.info("Data Classifier Initialization")
        self.data = data_frame
        self.db_ops = DatabaseOps()
        self.file_ops = ClassifierFileOps(bucket_name)
        self.tags = self.db_ops.get_tags()
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        self.data['timestamp'] = self.data['timestamp'].dt.floor('min')
        pass

    def __find_tag(self, tag_name: str) -> dict:
        """
        Finds a tag with the given tag_name in the list of tags.
        If not exists records the tag in the database.

        Parameters:
            tag_name (str): The name of the tag to find.

        Returns:
            dict: The tag dictionary if found, otherwise None.
        """
        for tag in self.tags:
            if tag['tag_name'] == tag_name:
                return tag
        self.tags = self.db_ops.insert_tag(tag_name)
        return self.__find_tag(tag_name)

    def __find_control_tag(self) -> dict:
        """
        Finds and returns the control tag from the list of tags.

        Returns:
            dict: The control tag dictionary if found, None otherwise.
        """
        for tag in self.tags:
            if tag['is_control_tag']:
                return tag
        return None

    def __find_batch_tag(self) -> dict:
        """
        Finds and returns the first tag in the list of tags that has the 'is_batch_name' attribute set to True.

        Returns:
            dict: The first tag with 'is_batch_name' attribute set to True, or None if no such tag is found.
        """
        for tag in self.tags:
            if tag['is_batch_name']:
                return tag
        return None

    def __find_lot_size_tag(self) -> dict:
        """
        Finds and returns the lot size tag from the list of tags.

        Returns:
            dict: The lot size tag if found, None otherwise.
        """
        for tag in self.tags:
            if tag['is_lot_size']:
                return tag
        return None

    def __process_batch_information(self, batch_related_df: pd.DataFrame, ignored_values: pd.DataFrame) -> pd.DataFrame:
        """
        Process the batch information in the given DataFrame and update the ignored_values DataFrame.

        Args:
            batch_related_df (pd.DataFrame): The DataFrame containing the batch-related data.
            ignored_values (pd.DataFrame): The DataFrame containing the ignored values.

        Returns:
            pd.DataFrame: The updated ignored_values DataFrame.
        """
        batch_related_df = batch_related_df.sort_values(
            by='timestamp').reset_index(drop=True)
        batch_related_df = DataCleaner(
            batch_related_df).clean_data(check_sort_order=False)
        latest_batch = self.db_ops.get_latest_batch()
        batch_tag = self.__find_batch_tag()
        lot_tag = self.__find_lot_size_tag()
        grouped_data = batch_related_df.groupby('tagname')
        data_frame_to_save = pd.DataFrame(
            columns=['tagname', 'timestamp', 'value'])
        if batch_tag['tag_name'] in grouped_data.groups.keys() \
                and not grouped_data.get_group(batch_tag['tag_name']).empty:
            for index, row in grouped_data.get_group(batch_tag['tag_name']).iterrows():
                if latest_batch and row['value'] == latest_batch['batch_name']:
                    ignored_values.loc[len(ignored_values)] = [
                        batch_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'Batch name already exists'
                    ]
                elif row['value'].isnumeric():
                    ignored_values.loc[len(ignored_values)] = [
                        batch_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'Corrupted Batch Name'
                    ]
                else:
                    self.db_ops.insert_batch(row['value'], row['timestamp'])
                    latest_batch = self.db_ops.get_latest_batch()
                    data_frame_to_save.loc[len(data_frame_to_save)] = [
                        batch_tag['tag_name'],
                        row['timestamp'],
                        row['value']]
        if lot_tag['tag_name'] in grouped_data.groups.keys() \
                and not grouped_data.get_group(lot_tag['tag_name']).empty:
            for index, row in grouped_data.get_group(lot_tag['tag_name']).iterrows():
                if latest_batch is None:
                    ignored_values.loc[len(ignored_values)] = [
                        lot_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'No batch found'
                    ]
                elif int(row['value']) == 0:
                    ignored_values.loc[len(ignored_values)] = [
                        lot_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'Lot size is zero'
                    ]
                elif (int(latest_batch['lot_size']) if latest_batch['lot_size'] else 0) == int(row['value']):
                    ignored_values.loc[len(ignored_values)] = [
                        lot_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'Lot size already exists'
                    ]
                else:
                    self.db_ops.update_batch_lot_size(
                        latest_batch['batch_name'], row['value'])
                    latest_batch = self.db_ops.get_latest_batch()
                    data_frame_to_save.loc[len(data_frame_to_save)] = [
                        lot_tag['tag_name'],
                        row['timestamp'],
                        row['value']
                    ]
        if not data_frame_to_save.empty:
            self.file_ops.save_batch_info_file(
                data=data_frame_to_save,
                batch_name=data_frame_to_save['value'].iloc[0])
        return ignored_values

    def __process_partition_tag(self, master_tag_df: pd.DataFrame, ignored_values: pd.DataFrame) -> pd.DataFrame:
        """
        Process the partition tag data.

        Args:
            master_tag_df (pd.DataFrame): The master tag data frame.
            ignored_values (pd.DataFrame): The ignored values data frame.

        Returns:
            pd.DataFrame: The updated ignored values data frame.
        """
        current_partition = self.db_ops.get_latest_partition()
        partition_tag = self.__find_control_tag()
        if current_partition:
            existing_batch_data = self.file_ops.get_existing_success_file(
                current_partition['id'], partition_tag['id'], partition_tag['tag_name'])
            if existing_batch_data is not None:
                master_tag_df = pd.concat([existing_batch_data, master_tag_df])
        master_tag_df['value'] = pd.to_numeric(
            master_tag_df['value'], errors='coerce').fillna(0)
        master_tag_df = DataCleaner(
            master_tag_df).clean_data(check_sort_order=False)
        data_frame_to_save = pd.DataFrame(
            columns=['tagname', 'timestamp', 'value', 'partition_id', 'tag_id'])
        for index, row in master_tag_df.iterrows():
            if current_partition:
                if row['value'] == 0 and not current_partition['end_time'] and current_partition['start_time'] < row['timestamp']:
                    partition_id = current_partition['id']
                    self.db_ops.end_partition(
                        partition_id=partition_id, end_time=row['timestamp'])
                    current_partition = None
                    ignored_values.loc[len(ignored_values)] = [
                        partition_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'Partition Terminated'
                    ]
                elif row['value'] == 0 and current_partition['end_time']:
                    ignored_values.loc[len(ignored_values)] = [
                        partition_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'Partition already ended'
                    ]
                elif row['value'] > 0:
                    if current_partition['end_time']:
                        self.db_ops.insert_partition(row['timestamp'])
                        current_partition = self.db_ops.get_latest_partition()
                    data_frame_to_save.loc[len(data_frame_to_save)] = [
                        partition_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        current_partition['id'],
                        partition_tag['id']
                    ]
            else:
                if row['value'] == 0:
                    ignored_values.loc[len(ignored_values)] = [
                        partition_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'No Active Partition and value is zero'
                    ]
                else:
                    self.db_ops.insert_partition(row['timestamp'])
                    current_partition = self.db_ops.get_latest_partition()
                    print(current_partition)
                    data_frame_to_save.loc[len(data_frame_to_save)] = [
                        partition_tag['tag_name'],
                        row['timestamp'],
                        row['value'],
                        current_partition['id'],
                        partition_tag['id']
                    ]
        if not data_frame_to_save.empty:
            partition_groups = data_frame_to_save.groupby('partition_id')
            for partition_id in partition_groups.groups.keys():
                data_to_persists = (DataCleaner(partition_groups.get_group(partition_id))
                                    .clean_data(check_sort_order=True))
                self.file_ops.save_success_file(
                    data=data_to_persists,
                    partition_id=partition_id,
                    tag_id=partition_tag['id'],
                    tag_name=partition_tag['tag_name']
                )
        return ignored_values

    def __process_tag_data(self, tag: str, tag_df: pd.DataFrame, ignored_values: pd.DataFrame) -> pd.DataFrame:
        """
        Process the tag data by cleaning and saving it.

        Args:
            tag (str): The tag name.
            tag_df (pd.DataFrame): The DataFrame containing the tag data.
            ignored_values (pd.DataFrame): The DataFrame to store ignored values.

        Returns:
            pd.DataFrame: The DataFrame containing the ignored values.
        """
        current_partition = self.db_ops.get_latest_partition()
        current_tag = self.__find_tag(tag)
        tag_df['value'] = pd.to_numeric(
            tag_df['value'], errors='coerce').fillna(0)
        tag_df = DataCleaner(tag_df).clean_data(check_sort_order=False)
        data_frame_to_save = pd.DataFrame(
            columns=['tagname', 'timestamp', 'value', 'partition_id', 'tag_id'])
        for index, row in tag_df.iterrows():
            if row['value'] == 0:
                ignored_values.loc[len(ignored_values)] = [
                    tag,
                    row['timestamp'],
                    row['value'],
                    'ignored',
                    'Value is zero'
                ]
            else:
                containing_partition = self.db_ops.find_containing_partition(timestamp=row['timestamp'])
                if not containing_partition:
                    ignored_values.loc[len(ignored_values)] = [
                        tag,
                        row['timestamp'],
                        row['value'],
                        'ignored',
                        'No partition found'
                    ]
                else:
                    data_frame_to_save.loc[len(data_frame_to_save)] = [
                        tag,
                        row['timestamp'],
                        row['value'],
                        containing_partition['id'],
                        current_tag['id']
                    ]
        if not data_frame_to_save.empty:
            tag_groups = data_frame_to_save.groupby('partition_id')
            for partition_id in tag_groups.groups.keys():
                data_to_persists = tag_groups.get_group(partition_id) \
                    if not self.file_ops.is_success_file_exists(partition_id=partition_id, tag_id=current_tag['id'], tag_name=current_tag['tag_name']) \
                    else pd.concat([
                        self.file_ops.get_existing_success_file(
                            partition_id, current_tag['id'], current_tag['tag_name']),
                        tag_groups.get_group(partition_id)])
                data_to_persists = (DataCleaner(tag_groups.get_group(partition_id))
                                    .clean_data(check_sort_order=True))
                self.file_ops.save_success_file(
                    data=data_to_persists,
                    partition_id=partition_id,
                    tag_id=current_tag['id'],
                    tag_name=current_tag['tag_name']
                )
        return ignored_values

    def classify_data(self) -> None:
        """
        Classifies the data by processing batch information and saving ignored values.

        Returns:
            None
        """
        logger.info("Classifying data")
        ignored_values = pd.DataFrame(
            columns=['tag_name', 'timestamp', 'value', 'status', 'reason'])
        batch_tag = self.__find_batch_tag()
        lot_tag = self.__find_lot_size_tag()
        tag_data = self.data.groupby('tagname')
        ignored_values = self.__process_batch_information(
            pd.concat([tag_data.get_group(batch_tag['tag_name']), tag_data.get_group(lot_tag['tag_name'])]), ignored_values)
        partition_control_tag = self.__find_control_tag()
        ignored_values = self.__process_partition_tag(
            tag_data.get_group(
                partition_control_tag['tag_name']), ignored_values)
        for tag in tag_data.groups.keys():
            if tag == batch_tag['tag_name'] or tag == lot_tag['tag_name'] or tag == partition_control_tag['tag_name']:
                continue
            else:
                ignored_values = self.__process_tag_data(
                    tag, tag_data.get_group(tag), ignored_values)
        self.file_ops.save_ignore_file(data=ignored_values)
        pass
