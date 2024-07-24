from tag_classifier import DataClassifier
from classifier_data_operations import DatabaseOps
from commons.file_operations import FileOps
import pandas as pd
import datetime


class ProcessData:
    """
    A class that processes data and performs various operations on it.

    Attributes:
        SUCCESS_FOLDER (str): The folder name for successful data.
        IGNORE_FOLDER (str): The folder name for ignored data.
        BATCH_INFO_FOLDER (str): The folder name for batch information.

    Methods:
        __init__(self, data, bucket_name): Initializes the ProcessData object.
        __get_batch_tag(self): Retrieves the batch tag from the list of all tags.
        __get_lot_size_tag(self): Retrieves the lot size tag from the list of all tags.
        __get_tag(self, tag_name): Retrieves a specific tag from the list of all tags.
        process(self): Processes the data and performs necessary operations.
    """

    SUCCESS_FOLDER = 'success'
    IGNORE_FOLDER = 'ignore'
    BATCH_INFO_FOLDER = 'batch_info'

    def __init__(self, data, bucket_name):
        """
        Initializes the ProcessData object.

        Args:
            data: The data to be processed.
            bucket_name: The name of the bucket.

        Returns:
            None
        """
        self.classifier = DataClassifier(csv_content=data)
        db_ops = DatabaseOps()
        self.all_tags = db_ops.get_tags()
        self.file_ops = FileOps(bucket_name)
        pass

    def __get_batch_tag(self):
        """
        Retrieves the batch tag from the list of all tags.

        Returns:
            The batch tag if found, None otherwise.
        """
        for tag in self.all_tags:
            if tag['is_batch_name']:
                return tag
        return None

    def __get_lot_size_tag(self):
        """
        Retrieves the lot size tag from the list of all tags.

        Returns:
            The lot size tag if found, None otherwise.
        """
        for tag in self.all_tags:
            if tag['is_lot_size']:
                return tag
        return None

    def __get_tag(self, tag_name):
        """
        Retrieves a specific tag from the list of all tags.

        Args:
            tag_name: The name of the tag to retrieve.

        Returns:
            The tag if found, None otherwise.
        """
        for tag in self.all_tags:
            if tag['tag_name'] == tag_name:
                return tag
        return None

    def process(self):
        """
        Processes the data and performs necessary operations.

        Returns:
            None
        """
        data_to_process, ignore_data = self.classifier.classify_data()
        ignore_filename = self.IGNORE_FOLDER + '/' + \
            datetime.datetime.now().strftime("%Y%m%d%H%M")
        self.file_ops.persist_data(
            data_frame=ignore_data,
            file_name=ignore_filename)
        lot_tag = self.__get_lot_size_tag()
        batch_tag = self.__get_batch_tag()
        batches = data_to_process[batch_tag['tag_name']].groupby('value')
        for batch, data in batches:
            file_name = batch
            latest_timestamp = data['timestamp'].max()
            lot_data = data_to_process[lot_tag['tag_name']][
                data_to_process[lot_tag['tag_name']]['timestamp'] <= latest_timestamp]
            data = pd.concat([data, lot_data]).drop_duplicates(keep=False)
            self.file_ops.persist_data(
                data_frame=data,
                file_name=self.BATCH_INFO_FOLDER + '/Batch_Name_' + file_name)
            print(file_name, data)
        data_to_process.pop(batch_tag['tag_name'])
        data_to_process.pop(lot_tag['tag_name'])
        for tag_name, data in data_to_process.items():
            tag = self.__get_tag(tag_name)
            if tag:
                partitions = data.groupby('partition_id')
                for partition_id in partitions.groups.keys():
                    data = partitions.get_group(partition_id)
                self.file_ops.persist_data(
                    data_frame=data,
                    file_name=self.SUCCESS_FOLDER + '/' + tag['tag_name'] + '_' + partition_id + '/' + tag['tag_name'] + '.csv')
