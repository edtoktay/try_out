import os
import pandas as pd
from tag_interpolate import TagInterpolate

if __name__ == '__main__':
    folder = '/home/deniz.toktay/Development/Workspaces/try_out/data-classifier/data/out/success/41_4'
    files = []
    for (dirpath, dirnames, filenames) in os.walk(folder):
        files.append(filenames)
    files.sort()
    for file in files[0]:
        file_name = folder + '/' + file
        df = TagInterpolate(pd.read_csv(file_name)).interpolate()
        print(df.head(50))
