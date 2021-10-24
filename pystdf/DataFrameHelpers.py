import numpy as np
import pandas as pd

def return_ecid_column(df):
    """
    Given a data frame with ECID columns, return ECID as series
    """
    try:
        ecid = df['lot_number'].map('{:.0f}'.format) + '_' + df['wafer_number'].map('{:.0f}'.format) + '_' + df['die_x'].map('{:.0f}'.format) + '_' + df['die_y'].map('{:.0f}'.format)
    except KeyError:
        print('One more more ECID columns are not present in the data frame')
        return np.NaN
    return ecid

def return_lot_wafer_column(df):
    """
    Given a data frame with ECID columns, return lot_wafer as series
    """
    try:
        lot_wafer = df['lot_number'].map('{:.0f}'.format) + '_' + df['wafer_number'].map('{:.0f}'.format)
    except KeyError:
        print('One more more ECID columns are not present in the data frame')
        return np.NaN
    return lot_wafer
