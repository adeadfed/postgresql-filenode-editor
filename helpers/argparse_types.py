import pathlib
import argparse
import base64
import csv
from io import StringIO
from pg_types import DataType


def Path(path):
    path = pathlib.Path(path)
    if not path.exists():
        raise argparse.ArgumentTypeError(
            'Supplied filenode path does not exist')
    if not path.is_file():
        raise argparse.ArgumentTypeError(
            'Supplied filenode path is not a file')
    return path


def Base64Data(b64_data):
    try:
        return base64.b64decode(b64_data)
    except Exception:
        raise argparse.ArgumentTypeError('Invalid base64 data supplied')


def CsvData(csv_data):
    try:
        return list(csv.reader(StringIO(csv_data)))[0]
    except Exception:
        raise argparse.ArgumentTypeError('Invalid CSV data supplied')


def DataTypeCsv(dtype_csv):
    try:
        return DataType(dtype_csv)
    except Exception:
        raise argparse.ArgumentTypeError('Invalid datatype CSV supplied')
