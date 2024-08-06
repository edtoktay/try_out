import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DataCleaner:
    """
    A class for cleaning data.

    Args:
        data_frame (pd.DataFrame): The input data frame to be cleaned.

    Attributes:
        data (pd.DataFrame): The cleaned data frame.

    Methods:
        clean_data: Cleans the data frame by removing duplicate timestamps and values.

    """

    def __init__(self, data_frame: pd.DataFrame) -> None:
        """
        Initialize the DataCleaner object.

        Parameters:
        - data_frame (pd.DataFrame): The input DataFrame to be cleaned.

        Returns:
        - None
        """
        self.data = data_frame
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], errors='coerce')
        self.data = self.data.dropna()
        self.data['timestamp'] = self.data['timestamp'].dt.floor('min')
        self.data = self.data.sort_values(
            by='timestamp').reset_index(drop=True)
        pass

    def __clear_duplicate_timestamps(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Clears duplicate timestamps from the given DataFrame.

        Args:
            data_frame (pd.DataFrame): The DataFrame containing the data.

        Returns:
            pd.DataFrame: The DataFrame with duplicate timestamps removed.
        """
        logger.info(
            f"Clear Duplicates Timestamps remove the timestamp with lower value")
        copy_df = data_frame.copy()
        field_to_evaluate = copy_df['timestamp']
        duplicated_sorted_values = copy_df[field_to_evaluate.isin(
            field_to_evaluate[field_to_evaluate.duplicated()])].sort_values(by=['timestamp', 'value'])
        if duplicated_sorted_values.empty:
            logger.info("No duplicated timestamps found")
            return data_frame
        duplicated_sorted_values = duplicated_sorted_values.drop_duplicates(
            subset='timestamp', keep='first')
        first_duplicated_removed = pd.concat(
            [copy_df, duplicated_sorted_values]).drop_duplicates(keep=False)
        if first_duplicated_removed.empty and not duplicated_sorted_values.empty:
            first_duplicated_removed = duplicated_sorted_values
        return self.__clear_duplicate_timestamps(data_frame=first_duplicated_removed)

    def __clear_duplicated_values(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Clears duplicated values in the given DataFrame.

        Args:
            data_frame (pd.DataFrame): The DataFrame to remove duplicated values from.

        Returns:
            pd.DataFrame: The DataFrame with duplicated values removed.
        """
        logger.info(
            f"Clear Duplicates Values remove the value earlier higher value")
        copy_df = data_frame.copy()
        field_to_evaluate = copy_df['value']
        duplicated_sorted_values = copy_df[field_to_evaluate.isin(
            field_to_evaluate[field_to_evaluate.duplicated()])].sort_values(by=['timestamp', 'value'])
        if duplicated_sorted_values.empty:
            logger.info("No duplicated values found")
            return data_frame
        duplicated_sorted_values = duplicated_sorted_values.drop_duplicates(
            subset=['value'], keep='last')
        first_duplicated_removed = pd.concat(
            [copy_df, duplicated_sorted_values]).drop_duplicates(keep=False)
        if first_duplicated_removed.empty and not duplicated_sorted_values.empty:
            first_duplicated_removed = duplicated_sorted_values
        return self.__clear_duplicated_values(data_frame=first_duplicated_removed)

    def __clear_non_incremental_values(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Clears non-incremental values from the given DataFrame.

        Args:
            data_frame (pd.DataFrame): The DataFrame to be processed.

        Returns:
            pd.DataFrame: The processed DataFrame with non-incremental values removed.
        """
        # Check if the 'value' column is incremental
        data_frame['value'] = pd.to_numeric(data_frame['value'], errors='coerce').fillna(0)
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

    def clean_data(self, check_sort_order: True) -> pd.DataFrame:
        logger.info("Cleaning data")
        return self.data\
            .pipe(self.__clear_duplicate_timestamps)\
            .pipe(self.__clear_duplicated_values)\
            .pipe(self.__clear_non_incremental_values) \
            if check_sort_order \
            else self.data\
            .pipe(self.__clear_duplicate_timestamps)\
            .pipe(self.__clear_duplicated_values)


if __name__ == "__main__":
    pass
