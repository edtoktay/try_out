import io
import pandas as pd
import numpy as np


class PandasOps:

    def create_csv_dataframe(self, csv_file):
        return pd.read_csv(csv_file)

    def create_dictionary_dataframe(self, dictionary_list):
        return pd.DataFrame(dictionary_list)
    
    def create_timeseries_datafarame(self, time_serie, columns):
        return pd.DataFrame(time_serie, columns=columns)

    def clear_duplicates(self, data_frame, field_name, sort_values_list=None):
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
            subset=field_name, keep='last', inplace=True)
        first_duplicate_removed = pd.merge(
            df,
            duplicated_sorted_values,
            on=sort_values_list
            if sort_values_list is not None
            else [field_name],
            how='outer',
            indicator=True
        ).query("_merge != 'both'").drop("_merge", axis=1).reset_index(drop=True)
        return self.clear_duplicates(data_frame=first_duplicate_removed,
                                     field_name=field_name,
                                     sort_values_list=sort_values_list)

    def interpolated_time_serie(self, timestamps, values):
        time_serie = pd.Series(values, index=timestamps)
        time_serie[time_serie == -1] = np.nan
        time_serie = time_serie.resample('min')
        return time_serie.interpolate(method='time')
