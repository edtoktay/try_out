from raw_data_gatherer import ProcessRawData

p = ProcessRawData('RawData/ED1108_2.csv')
b = p.process_csv_file()
print(b)
