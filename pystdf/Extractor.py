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


def write_data_frame(filename, output_dir=None, output_file_type=None, format_test_name=True):
    reopen_fn, open_file = make_function(filename)
    base_filename = os.path.basename(filename)
    output_filename = f'{base_filename}_out-detailed.csv'
    if os.path.exists(output_filename):
        os.remove(output_filename)
    p = Parser(inp=open_file, reopen_fn=reopen_fn)
    p.addSink(
        DataFrameWriter(input_file=filename, output_dir=output_dir, output_file_type=output_file_type, format_test_name=format_test_name)
    )
    p.parse()
    open_file.close()


def test():
    f = '/home/nnguyen/Projects/pystdf_lite/data/M1BB-A0-W0---_4C67397-01_W05_20210716_184043_Datalog.stdf.gz'
    output_dir = os.path.dirname(f)
    write_data_frame(filename=f, output_dir=output_dir, output_file_type='csv')

