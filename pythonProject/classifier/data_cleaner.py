import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DataCleaner:
    """
    A class for cleaning data.

    Args:
        csv_content (str): The content of the CSV file.

    Attributes:
        data (pandas.DataFrame): The cleaned data.

    Methods:
        clean: Performs the data cleaning logic.

    """

    def __init__(self, csv_content):
        """
        Initializes a DataCleaner object.

        Args:
            csv_content (str): The content of the CSV file.

        """
        logger.info("DataCleaner object created")
        self.data = pd.read_csv(
            csv_content,
            usecols=[0, 1, 2],
            names=[
                'tagname',
                'timestamp',
                'value'])
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        self.data['timestamp'] = self.data['timestamp'].dt.floor('min')
        self.data = self.data.sort_values(
            by='timestamp').reset_index(drop=True)
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])

    def __clear_duplicates(self, data_frame, field_name, sort_values_list, is_keep_last=True):
        """
        Clears duplicate values in a DataFrame.

        Args:
            data_frame (pandas.DataFrame): The DataFrame to remove duplicates from.
            field_name (str): The name of the field to evaluate for duplicates.
            sort_values_list (list): The list of fields to sort the DataFrame by.
            is_keep_last (bool, optional): Whether to keep the last duplicate or the first duplicate. Defaults to True.

        Returns:
            pandas.DataFrame: The DataFrame with duplicates removed.

        """
        copy_df = data_frame.copy()
        field_to_evaluate = copy_df[field_name]
        duplicated_sorted_values = copy_df[
            field_to_evaluate.isin(
                field_to_evaluate[field_to_evaluate.duplicated()])
        ].sort_values(by=sort_values_list if sort_values_list else field_name)
        if duplicated_sorted_values.empty:
            return data_frame
        duplicated_sorted_values.drop_duplicates(
            subset=field_name,
            keep='last' if is_keep_last else 'first',
            inplace=True
        )
        first_duplicated_removed = pd.merge(
            copy_df,
            duplicated_sorted_values,
            on=sort_values_list if sort_values_list else [field_name],
            how='outer',
            indicator=True
        ).query('_merge != "both"').drop(columns='_merge', axis=1).reset_index(drop=True)
        if first_duplicated_removed.empty and not duplicated_sorted_values.empty:
            first_duplicated_removed = duplicated_sorted_values
        else:
            if 'tagname_y' in first_duplicated_removed.columns:
                first_duplicated_removed = first_duplicated_removed.drop('tagname_y', axis=1)
            if 'tagname_x' in first_duplicated_removed.columns:
                first_duplicated_removed = first_duplicated_removed.rename(columns={'tagname_x': 'tagname'})
        return self.__clear_duplicates(
            data_frame=first_duplicated_removed,
            field_name=field_name,
            sort_values_list=sort_values_list,
            is_keep_last=is_keep_last
        )

    def __clean(self, data_frame):
        """
        Performs the data cleaning logic.

        Returns:
            pandas.DataFrame: The cleaned data.

        """
        # Clear duplicate timestamps
        data_frame = self.__clear_duplicates(
            data_frame=data_frame,
            field_name='timestamp',
            sort_values_list=['timestamp', 'value'],
            is_keep_last=False
        )
        # Clear duplicate values
        data_frame = self.__clear_duplicates(
            data_frame=data_frame,
            field_name='value',
            sort_values_list=['timestamp', 'value']
        )
        return data_frame

    def get_tag_data(self):
        """
        Retrieves tag data from the DataFrame and returns a dictionary
        where each tag is mapped to its cleaned data.

        Returns:
            dict: A dictionary where each tag is mapped to its cleaned data.
        """
        tag_partitioned_data = {}
        grouped_df = self.data.groupby('tagname')
        for tag, data in grouped_df:
            x = self.__clean(data)
            tag_partitioned_data[tag] = x
        return tag_partitioned_data


if __name__ == "__main__":
    pass
