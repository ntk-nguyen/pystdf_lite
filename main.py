# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from pystdf.Extractor import write_data_frame

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    file_names = [
        r'C:\Users\Nguyen Nguyen\Documents\PythonProjects\pystdf\data\lot2.stdf.gz',
        r'C:\Users\Nguyen Nguyen\Downloads\stress_stdfs\M1BB-A0-W0---_4C66250-01_W01_20210520_230258_Datalog.stdf.gz',
    ]
    for f in file_names:
        write_data_frame(f)
