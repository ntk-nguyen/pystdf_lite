# This is the main function to extract STDF files.
# Running main.py will parse sample public STDF files in /data folder

import glob
import os
from pystdf.Extractor import write_data_frame

if __name__ == '__main__':
    sample_data_dir = os.path.join(os.getcwd(), 'data')
    file_names = glob.glob(os.path.join(sample_data_dir, '*.stdf.gz'))
    for f in file_names:
        write_data_frame(f, output_file_type='csv', parameter_expression=r'ecid_read')
