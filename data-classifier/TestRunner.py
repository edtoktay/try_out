import os
import pandas as pd
from tag_data_classifier import DataClassifier


def read_file(file_path):
    return pd.read_csv(file_path, usecols=[0, 1, 2], names=['tagname', 'timestamp', 'value'])


def call_classifier(path):
    folder = '/home/deniz.toktay/Development/Workspaces/try_out/data-classifier/data/in/' + path
    print(folder)
    files = []
    for (dirpath, dirnames, filenames) in os.walk(folder):
        files.append(filenames)
    files.sort()
    print(files)
    for file in files[0]:
        file_name = folder + '/' + file
        DataClassifier(read_file(file_name), 'test_bucket').classify_data()
    pass


if __name__ == '__main__':
    call_classifier('08')
    call_classifier('09')
    call_classifier('10')
    call_classifier('11')
    call_classifier('12')
    pass
