import os
import re
import gzip
import bz2
from pystdf.IO import Parser
from pystdf.Writers import DataFrameWriter

gzPattern = re.compile('\\.g?z', re.I)
bz2Pattern = re.compile('\\.bz2', re.I)


def make_function(filename):
    # Defining functions
    if gzPattern.search(filename):
        def _function():
            return gzip.open(filename, 'rb')
    elif bz2Pattern.search(filename):
        def _function():
            return bz2.BZ2File(filename, 'rb')
    else:
        _function = None
    # Defining open instance
    if _function is None:
        f = open(filename, 'rb')
    else:
        f = _function()
    return _function, f


def write_data_frame(filename):
    reopen_fn, open_file = make_function(filename)
    base_filename = os.path.basename(filename)
    output_filename = f'{base_filename}_out-detailed.csv'
    if os.path.exists(output_filename):
        os.remove(output_filename)
    p = Parser(inp=open_file, reopen_fn=reopen_fn)
    p.addSink(
        DataFrameWriter(input_file=filename)
    )
    p.parse()
    open_file.close()


def test():
    file_names = [
        r'C:\Users\Nguyen Nguyen\Documents\PythonProjects\pystdf\data\lot2.stdf.gz',
        # r'C:\Users\Nguyen Nguyen\Downloads\stress_stdfs\M1BB-A0-W0---_4C66250-01_W01_20210520_230258_Datalog.stdf.gz',
    ]
    import glob
    file_names = glob.glob(r'C:\Users\Nguyen Nguyen\Downloads\stress_stdfs\*.std*')
    for f in file_names:
        write_data_frame(f)
