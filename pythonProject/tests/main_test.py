import pathlib
from classifier.process_data import ProcessData

file_location = pathlib.Path(__file__).parent.absolute()
p = ProcessData(data=file_location / 'hede')
p.process()
