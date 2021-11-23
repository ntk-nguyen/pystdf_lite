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
from pystdf import V4, DataFrameHelpers
from pystdf.Types import FieldNames
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

    def __init__(self, input_file, output_dir=None, output_file_type=None, format_test_name=True):
        supported_files = ['csv', 'parquet']
        self.input_file = input_file
        if output_dir is None:
            self.output_dir = os.path.dirname(self.input_file)
        else:
            self.output_dir = output_dir
        if output_file_type not in supported_files:
            self.output_file_type = 'csv'
        else:
            self.output_file_type = output_file_type
        self.input_filename = os.path.basename(self.input_file).replace('.gz', '').replace('.stdf', '').\
            replace('.std', '')
        self.output_file = os.path.join(
            self.output_dir, f'{self.input_filename}.{self.output_file_type}'
        )
        self.format_test_name = format_test_name
        self.bin_file = os.path.join(self.output_dir, f'{self.input_filename}-bin.csv')
        self.limit_file = os.path.join(self.output_dir, f'{self.input_filename}-limits.csv')
        self.meta_file = os.path.join(self.output_dir, f'{self.input_filename}-meta.json')
        self.CURR_SQ = None
        self.start_timestamp = None
        self.end_timestamp = None
        self.file_name = None
        self.temperature = None
        self.pir_columns = [FieldNames.head_number, FieldNames.site_number]
        self.ptr_columns = [FieldNames.head_number, FieldNames.site_number, FieldNames.result, FieldNames.test_text, 
                            FieldNames.test_number, FieldNames.resolution_scale, FieldNames.low_limit_scale, 
                            FieldNames.high_limit_scale, FieldNames.low_limit, FieldNames.high_limit, FieldNames.units, 
                            FieldNames.low_spec, FieldNames.high_spec, 'index']
        self.prr_columns = [FieldNames.head_number, FieldNames.site_number, FieldNames.part_flag, FieldNames.num_test, 
                            FieldNames.hard_bin, FieldNames.soft_bin, FieldNames.x_coordinate, FieldNames.y_coordinate, 
                            FieldNames.test_time, FieldNames.part_id, 'index']
        self.limit_columns = [FieldNames.test, FieldNames.resolution_scale, FieldNames.low_limit_scale, 
                              FieldNames.high_limit_scale, FieldNames.low_limit, FieldNames.high_limit, 
                              FieldNames.units, FieldNames.low_spec, FieldNames.high_spec]
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
        self.meta_data[FieldNames.wafer_id] = None
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
        if data[0].__class__.__name__.lower() == 'bps' and FieldNames.sequence_name in data[0].fieldNames:
            fmt_val = self.csv_format(data[0], data[0].fieldNames.index(FieldNames.sequence_name),
                                      data[1][data[0].fieldNames.index(FieldNames.sequence_name)])
            self.CURR_SQ = quoteattr(fmt_val, self.extra_entities)
        elif data[0].__class__.__name__.lower() == 'pir':
            self.part_count += 1
            selected_data = {}
            for c in self.pir_columns:
                selected_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                   data[1][data[0].fieldNames.index(c)]).replace('\t', '')
            self.part_id_dict[f"{selected_data[FieldNames.head_number]}-{selected_data[FieldNames.site_number]}"] = \
                selected_data['index'] = self.part_count
        elif data[0].__class__.__name__.lower() == 'ptr':
            selected_data = {}
            for c in [p for p in self.ptr_columns if p != 'index']:
                selected_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                   data[1][data[0].fieldNames.index(c)]).replace('\t', '')
            selected_data['index'] = self.part_id_dict[
                f"{selected_data[FieldNames.head_number]}-{selected_data[FieldNames.site_number]}"
            ]
            self.ptr_data.append('\t'.join([f'{selected_data[c]}' for c in selected_data.keys()]))
        elif data[0].__class__.__name__.lower() == 'prr':
            self.prr_count += 1
            selected_data = {}
            for c in [p for p in self.prr_columns if p != 'index']:
                selected_data[c] = self.csv_format(data[0], data[0].fieldNames.index(c),
                                                   data[1][data[0].fieldNames.index(c)]).replace('\t', '')
            selected_data['index'] = self.part_id_dict[
                f"{selected_data[FieldNames.head_number]}-{selected_data[FieldNames.site_number]}"
            ]
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
            self.meta_data[FieldNames.wafer_id] = \
            self.csv_format(data[0], data[0].fieldNames.index(FieldNames.wafer_id), \
            data[1][data[0].fieldNames.index(FieldNames.wafer_id)]).replace('\t', '')
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
        hbr_df[FieldNames.hardbin_number] = hbr_df[FieldNames.hardbin_number].astype(int)
        sbr_df = pd.read_csv(io.StringIO('\n'.join(self.summary_data['sbr'])), sep='\t')
        hbr_df[FieldNames.softbin_number] = hbr_df[FieldNames.hardbin_number].astype(int)
        hbr_df.drop_duplicates(subset=[FieldNames.hardbin_number, FieldNames.hardbin_name], keep='last', inplace=True)
        hbr_df = hbr_df[[FieldNames.hardbin_number, FieldNames.hardbin_name, FieldNames.hardbin_passfail]]
        sbr_df.drop_duplicates(subset=[FieldNames.softbin_number, FieldNames.softbin_name], keep='last', inplace=True)
        sbr_df = sbr_df[[FieldNames.softbin_number, FieldNames.softbin_name, FieldNames.softbin_passfail]]
        # Remove data after the last space in test names 
        # For example: OPENS ATEST_FORCE 386
        # Replace period with one underscore and space with two underscore
        if self.format_test_name:
            ptr_df[FieldNames.test_text] = ptr_df[FieldNames.test_text].str.rsplit(' ', n=1, expand=True)[0].replace('.', '_').replace(' ', '__', regex=True).str.lower()
        # Concatenate test numbers and test names
        ptr_df[FieldNames.test] = ptr_df[FieldNames.test_number].astype(str) + '__' + ptr_df[FieldNames.test_text]
        # Limit data frame
        limits_df = ptr_df[self.limit_columns].copy()
        limits_df.drop_duplicates(subset=[FieldNames.test], inplace=True)
        limits_df[FieldNames.job_name] = self.meta_data[FieldNames.job_name]
        limits_df[FieldNames.job_rev] = self.meta_data[FieldNames.job_rev]
        limits_df.columns = map(str.lower, limits_df.columns)
        limits_df.to_csv(self.limit_file, index=False)
        ptr_df.drop(labels=[p for p in self.limit_columns if p != FieldNames.test], axis=1, inplace=True)
        # Transform from long to wide tables
        index_columns = [p for p in ptr_df.columns if p not in [FieldNames.test, FieldNames.test_number, FieldNames.test_text, FieldNames.result]]
        ptr_df = pd.pivot(ptr_df, index=index_columns, columns=FieldNames.test, values=FieldNames.result).reset_index()
        df = pd.merge(ptr_df, prr_df, on=['index', FieldNames.head_number, FieldNames.site_number], how='outer')
        # Change Hard Bin and Soft Bin column names and merge to HBR and SBR
        df.rename(columns={FieldNames.hard_bin: FieldNames.hardbin_number, FieldNames.soft_bin: FieldNames.softbin_number}, inplace=True)
        for c in [FieldNames.hardbin_number, FieldNames.softbin_number]:
            df.loc[pd.isna(df[c]), c] = -1
            df[c] = df[c].astype(int)
        # Updated November 03, 2021 to fix an issue where hbr and sbr are missing.
        df = pd.merge(df, hbr_df, on=[FieldNames.hardbin_number], how='left')
        df = pd.merge(df, sbr_df, on=[FieldNames.softbin_number], how='left')
        # Adding some more meta data columns
        meta_classes_for_ptr = ['far', 'mir', 'mrr']
        time_columns = [FieldNames.setup_time, FieldNames.start_time, FieldNames.finish_time]
        for k in meta_classes_for_ptr:
            columns = V4.data_classes[k].fieldNames
            for c in columns:
                if c in time_columns:
                    try:
                        df[c] = pd.to_datetime(datetime.strptime(self.meta_data[c], '%H:%M:%ST%d-%b-%Y'))
                    except TypeError:
                        pass
                else:
                    df[c] = self.meta_data[c]
        # Adding WAFER_ID
        df[FieldNames.wafer_id] = self.meta_data[FieldNames.wafer_id]
        # Formatting data frame
        integer_columns = [FieldNames.head_number, FieldNames.site_number, FieldNames.num_test, FieldNames.part_id, FieldNames.test_time, FieldNames.hardbin_number, FieldNames.softbin_number, FieldNames.part_flag]
        for c in integer_columns:
            df.loc[pd.isna(df[c]), c] = -1
            df[c] = df[c].astype(int)
        df['file'] = self.input_filename
        df.sort_values(by=[FieldNames.part_id], ascending=True, inplace=True)
        try:
            lot_column = [p for p in df.columns if re.search('[0-9]+', p) and p.lower().__contains__('ecid_read') and p.lower().__contains__('lot')][0]
            df[FieldNames.lot_number] = df[lot_column]
        except IndexError:
            df[FieldNames.lot_number] = np.NaN
        try:
            wafer_column = [p for p in df.columns if re.search('[0-9]+', p) and p.lower().__contains__('ecid_read') and p.lower().__contains__('wafer')][0]
            df[FieldNames.wafer_number] = df[wafer_column]
        except IndexError:
            df[FieldNames.wafer_number] = np.NaN
        try:
            diex_column = [p for p in df.columns if re.search('[0-9]+', p) and p.lower().__contains__('ecid_read') and p.lower().__contains__('coord') and p.lower().__contains__('x')][0]
            df[FieldNames.die_x] = df[diex_column]
        except IndexError:
            df[FieldNames.die_x] = np.NaN
        try:
            diey_column = [p for p in df.columns if re.search('[0-9]+', p) and p.lower().__contains__('ecid_read') and p.lower().__contains__('coord') and p.lower().__contains__('y')][0]
            df[FieldNames.die_y] = df[diey_column]
        except IndexError:
            df[FieldNames.die_y] = np.NaN
        ecid_columns = [FieldNames.lot_number, FieldNames.wafer_number, FieldNames.die_x, FieldNames.die_y]
        df[FieldNames.ecid] = DataFrameHelpers.return_ecid_column(df[ecid_columns])
        # Saving bin and parametric data frame
        meta_columns = [p for p in df.columns if not (re.search('[0-9]+', p) or p == 'index')]
        updated_meta_columns = [p.lower().replace('.', '_').replace(' ', '_') for p in meta_columns]
        meta_columns_dict = {p: p.lower().replace('.', '_').replace(' ', '_') for p in meta_columns}
        df.rename(columns=meta_columns_dict, inplace=True)
        parm_columns = [p for p in df.columns if re.search('[0-9]+', p)]
        df[updated_meta_columns].to_csv(self.bin_file, index=False)
        if self.output_file_type == 'csv':
            df[updated_meta_columns + parm_columns].to_csv(self.output_file, index=False)
        else:
            df[updated_meta_columns + parm_columns].to_parquet(self.output_file, index=False)
