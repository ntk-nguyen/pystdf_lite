# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import glob
import os
from pystdf.Extractor import write_data_frame

if __name__ == '__main__':
    sample_data_dir = os.path.join(os.getcwd(), 'data')
    file_names = glob.glob(os.path.join(sample_data_dir, '*.stdf.gz'))
    for f in file_names:
        write_data_frame(f, output_file_type='parquet')
