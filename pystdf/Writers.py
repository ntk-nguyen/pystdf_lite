#
# PySTDF - The Pythonic STDF Parser
# Copyright (C) 2006 Casey Marshall
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
import json
import os.path
import re
from time import strftime, localtime
from xml.sax.saxutils import quoteattr
from pystdf import V4
import io
import numpy as np
import pandas as pd
from datetime import datetime


def format_by_type(value, field_type):
    if field_type in ('B1', 'N1'):
        return '%02X' % value
    else:
        return str(value)


class DataFrameWriter:
    extra_entities = {'\0': ''}

    @staticmethod
    def csv_format(rectype, field_index, value):
        field_type = rectype.fieldStdfTypes[field_index]
        if value is None:
            return ""
        elif rectype is V4.gdr:
            return ';'.join([str(v) for v in value])
        elif field_type[0] == 'k':  # An Array of some other type
            return ','.join([format_by_type(v, field_type[2:]) for v in value])
        elif rectype is V4.mir or rectype is V4.mrr:
            field_name = rectype.fieldNames[field_index]
            if field_name.endswith('_T'):  # A Date-Time in an MIR/MRR
                return strftime('%H:%M:%ST%d-%b-%Y', localtime(value))
            else:
                return str(value)
        else:
            return str(value)

    def __init__(self, input_file, output_dir=None, output_file_type=None):
        supported_files = ['csv', 'parquet']
        self.input_file = input_file
        if output_dir is None:
            self.output_dir = os.path.dirname(self.input_file)
        else:
            self.output_dir = output_dir
        if output_file_type not in supported_files:
            self.output_file_type = 'csv'
        else:
            self.output_file_type = 'parquet'
        self.input_filename = os.path.basename(self.input_file).replace('.gz', '').replace('.stdf', '').\
            replace('.std', '')
        self.output_file = os.path.join(
            self.output_dir, f'{self.input_filename}.{self.output_file_type}'
        )
        self.limit_file = os.path.join(self.output_dir, f'{self.input_filename}-limits.csv')
        self.meta_file = os.path.join(self.output_dir, f'{self.input_filename}-meta.json')
        self.CURR_SQ = None
        self.start_timestamp = None
        self.end_timestamp = None
        self.file_name = None
        self.temperature = None
        self.pir_columns = ['HEAD_NUM', 'SITE_NUM']
        self.ptr_columns = ['HEAD_NUM', 'SITE_NUM', 'RESULT', 'TEST_TXT', 'TEST_NUM',
                            'RES_SCAL', 'LLM_SCAL', 'HLM_SCAL', 'LO_LIMIT', 'HI_LIMIT', 'UNITS', 'LO_SPEC', 'HI_SPEC',
                            'index']
        self.prr_columns = ['HEAD_NUM', 'SITE_NUM', 'PART_FLG', 'NUM_TEST', 'HARD_BIN', 'SOFT_BIN',
                            'X_COORD', 'Y_COORD', 'TEST_T', 'PART_ID', 'index']
        self.limit_columns = ['TEST', 'RES_SCAL', 'LLM_SCAL', 'HLM_SCAL', 'LO_LIMIT', 'HI_LIMIT', 'UNITS',
                              'LO_SPEC', 'HI_SPEC']
        self.ptr_output_columns = self.prr_columns + [p for p in self.ptr_columns if p not in self.prr_columns]
        self.ptr_data = ['\t'.join(self.ptr_columns)]
        self.prr_data = ['\t'.join(self.prr_columns)]
        self.summary_classes = ['sbr', 'hbr', 'pcr']
        self.summary_data = {p: ['\t'.join(V4.data_classes[p].fieldNames)] for p in self.summary_classes}
        self.part_id_dict = {}
        self.part_count = 0
        self.prr_count = 0
        self.meta_classes = ['far', 'mir', 'mrr', 'wcr']
        self.meta_data = {c: None for p in self.meta_classes for c in V4.data_classes[p].fieldNames}
        self.meta_data['WAFER_ID'] = None
        self.meta_data['filename'] = self.input_filename

    def before_begin(self, data_source):
        print(f'Extracting {self.input_filename} begins')

    def after_send(self, data_source, data):
        # Hierarchical structure for STDF files PIR -> PTR -> PRR
        # - PIR
        # Frequency: one per part tested
        # Location: Anywhere in the data stream after the initial sequence, and before the corresponding PRR.
        # Sent before testing each part.
        # - PTR
        # HEAD_NUM, SITE_NUM
        # If a test system does not support parallel testing, and does not have a standard way of identifying its single
        # test site or head, these fields should be set to 1.
        # When parallel testing, these fields are used to associate individual datalogged results with a PIR/PRR pair.
        # A PTR belongs to the PIR/PRR pair having the same values for HEAD_NUM and SITE_NUM.
        # - PRR
        # Frequency: one per part tested
        # Location: Anywhere in the data stream after the corresponding PIR and before the MRR. Sent after completion of
        # testing each part.

        if data[0].__class__.__name__.lower() == 'bps' and 'SEQ_NAME' in data[0].fieldNames:
            fmt_val = self.csv_format(data[0], data[0].fieldNames.index("SEQ_NAME"),
                                      data[1][data[0].fieldNames.index("SEQ_NAME")])
            self.CURR_SQ = quoteattr(fmt_val, self.extra_entities)
        elif data[0].__class__.__name__.lower() == 'pir':
            self.part_count += 1
            selected_data = {}
            for c in self.pir_columns:
                selected_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                   data[1][data[0].fieldNames.index(c)]).replace('\t', '')
            self.part_id_dict[f"{selected_data['HEAD_NUM']}-{selected_data['SITE_NUM']}"] = selected_data['index'] = \
                self.part_count
        elif data[0].__class__.__name__.lower() == 'ptr':
            selected_data = {}
            for c in [p for p in self.ptr_columns if p != 'index']:
                selected_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                   data[1][data[0].fieldNames.index(c)]).replace('\t', '')
            selected_data['index'] = self.part_id_dict[f"{selected_data['HEAD_NUM']}-{selected_data['SITE_NUM']}"]
            self.ptr_data.append('\t'.join([f'{selected_data[c]}' for c in selected_data.keys()]))
        elif data[0].__class__.__name__.lower() == 'prr':
            self.prr_count += 1
            selected_data = {}
            for c in [p for p in self.prr_columns if p != 'index']:
                selected_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                   data[1][data[0].fieldNames.index(c)]).replace('\t', '')
            selected_data['index'] = self.part_id_dict[f"{selected_data['HEAD_NUM']}-{selected_data['SITE_NUM']}"]
            self.prr_data.append('\t'.join([f'{selected_data[c]}' for c in selected_data.keys()]))
        elif data[0].__class__.__name__.lower() in self.summary_classes:
            data_class = data[0].__class__.__name__.lower()
            selected_data = {}
            for c in V4.data_classes[data_class].fieldNames:
                selected_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                   data[1][data[0].fieldNames.index(c)]).replace('\t', '')
            self.summary_data[data_class].append('\t'.join([f'{selected_data[c]}' for c in selected_data.keys()]))
        elif data[0].__class__.__name__.lower() in self.meta_classes:
            data_class = data[0].__class__.__name__.lower()
            for c in V4.data_classes[data_class].fieldNames:
                self.meta_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                    data[1][data[0].fieldNames.index(c)]).replace('\t', '')
        elif data[0].__class__.__name__.lower() == 'wir':
            c = 'WAFER_ID'
            self.meta_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                data[1][data[0].fieldNames.index(c)]).replace('\t', '')
        # self.stream.write('/>\n')

    def after_complete(self, data_source):
        self.post_processing()
        print('Extracting completes')

    def post_processing(self):
        with open(self.meta_file, 'w') as f:
            json.dump(self.meta_data, f)
        # Merge PTR and PRR data
        ptr_df = pd.read_csv(io.StringIO('\n'.join(self.ptr_data)), sep='\t')
        prr_df = pd.read_csv(io.StringIO('\n'.join(self.prr_data)), sep='\t')
        hbr_df = pd.read_csv(io.StringIO('\n'.join(self.summary_data['hbr'])), sep='\t')
        sbr_df = pd.read_csv(io.StringIO('\n'.join(self.summary_data['sbr'])), sep='\t')
        hbr_df.drop_duplicates(subset=['HBIN_NUM', 'HBIN_NAM'], keep='last', inplace=True)
        hbr_df = hbr_df[['HBIN_NUM', 'HBIN_NAM', 'HBIN_PF']]
        sbr_df.drop_duplicates(subset=['SBIN_NUM', 'SBIN_NAM'], keep='last', inplace=True)
        sbr_df = sbr_df[['SBIN_NUM', 'SBIN_NAM', 'SBIN_PF']]
        # Concatenate test numbers and test names
        ptr_df['TEST'] = ptr_df['TEST_NUM'].astype(str) + ':' + ptr_df['TEST_TXT']
        # Limit data frame
        limits_df = ptr_df[self.limit_columns].copy()
        limits_df.drop_duplicates(subset=['TEST'], inplace=True)
        limits_df['JOB_NAM'] = self.meta_data['JOB_NAM']
        limits_df['JOB_REV'] = self.meta_data['JOB_REV']
        limits_df.to_csv(self.limit_file, index=False)
        ptr_df.drop(labels=[p for p in self.limit_columns if p != 'TEST'], axis=1, inplace=True)
        # Transform from long to wide tables
        index_columns = [p for p in ptr_df.columns if p not in ['TEST', 'TEST_NUM', 'TEST_TXT', 'RESULT']]
        ptr_df = pd.pivot(ptr_df, index=index_columns, columns='TEST', values='RESULT').reset_index()
        df = pd.merge(ptr_df, prr_df, on=['index', 'HEAD_NUM', 'SITE_NUM'], how='outer')
        # Change Hard Bin and Soft Bin column names and merge to HBR and SBR
        df.rename(columns={'HARD_BIN': 'HBIN_NUM', 'SOFT_BIN': 'SBIN_NUM'}, inplace=True)
        df = pd.merge(df, hbr_df, on=['HBIN_NUM'], how='inner')
        df = pd.merge(df, sbr_df, on=['SBIN_NUM'], how='inner')
        # Adding some more meta data columns
        meta_classes_for_ptr = ['far', 'mir', 'mrr']
        time_columns = ['SETUP_T', 'START_T', 'FINISH_T']
        for k in meta_classes_for_ptr:
            columns = V4.data_classes[k].fieldNames
            for c in columns:
                if c in time_columns:
                    df[c] = pd.to_datetime(datetime.strptime(self.meta_data[c], '%H:%M:%ST%d-%b-%Y'))
                else:
                    df[c] = self.meta_data[c]
        # Adding WAFER_ID
        df['WAFER_ID'] = self.meta_data['WAFER_ID']
        # Formatting data frame
        integer_columns = ['HEAD_NUM', 'SITE_NUM', 'NUM_TEST', 'PART_ID', 'TEST_T', 'HBIN_NUM', 'SBIN_NUM']
        for c in integer_columns:
            df[c] = df[c].astype(int)
        df['file'] = self.input_filename
        df.sort_values(by=['PART_ID'], ascending=True, inplace=True)
        try:
            wafer_column = [p for p in df.columns if re.search('[0-9]+', p) and p.lower().__contains__('wafer')][0]
            df['wafer_number'] = df[wafer_column]
        except IndexError:
            df['wafer_number'] = np.NaN
        try:
            diex_column = [p for p in df.columns if re.search('[0-9]+', p) and p.lower().__contains__('coord') and p.lower().__contains__('x')][0]
            df['die_x'] = df[diex_column]
        except IndexError:
            df['die_x'] = np.NaN
        try:
            diey_column = [p for p in df.columns if re.search('[0-9]+', p) and p.lower().__contains__('coord') and p.lower().__contains__('y')][0]
            df['die_y'] = df[diey_column]
        except IndexError:
            df['die_y'] = np.NaN
        # Saving data frame
        meta_columns = [p for p in df.columns if not (re.search('[0-9]+', p) or p == 'index')]
        parm_columns = [p for p in df.columns if re.search('[0-9]+', p)]
        if self.output_file_type == 'csv':
            df[meta_columns + parm_columns].to_csv(self.output_file, index=False)
        else:
            df[meta_columns + parm_columns].to_parquet(self.output_file, index=False)
