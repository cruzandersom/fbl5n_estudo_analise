import io
import logging
import re
import string
from datetime import datetime, date
from typing import Union, Tuple
import pandas as pd
import sys
import pathlib
import random

sys.path.append(str(pathlib.Path(__file__).parent.absolute()))

from s3 import get_file_from_s3, send_file_to_s3, copy_object_in_s3, delete_file_from_s3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def read_file(s3_client, bucket: str, key: str) -> bytes:
    LOGGER.info(f'AWS lambda - Reading File of {bucket} in {key}')

    return get_file_from_s3(s3_client, bucket, key)


def create_parquet_and_send_to_s3(s3_client, bucket: str, key: str, df: pd.DataFrame):
    LOGGER.info(f'AWS lambda - Creating and Uploading Parquet File to {key}')

    with io.BytesIO() as buffer:
        df.to_parquet(buffer, index=False)
        send_file_to_s3(s3_client, bucket, key, buffer)


def create_parquet_key(file_date: date,
                       system_name: str,
                       database: str,
                       table_name: str) -> str:

    year = file_date.strftime('%Y')
    month = file_date.strftime('%m')
    day = file_date.strftime('%d')

    file_name = f'{file_date.strftime("%Y%m%d")}-{table_name}.parquet'

    return f'semi-treated/{system_name}/{database}/{table_name}/{year}/{month}/{day}/{file_name}'


def parse_sap_format_to_number(number: str) -> Union[float, None]:
    number_converted = number.replace('.', '').replace(',', '.')

    if len(number_converted) == 0:
        return None

    if number_converted[-1] == '-':
        return -1 * float(number_converted[:len(number_converted) - 1])

    return float(number_converted)


def move_file_to_final_state(s3_client,
                             bucket: str,
                             system_name: str,
                             database: str,
                             key: str,
                             state: str) -> None:
    new_key = f'raw-data/{system_name}/{database}/{state}/{key.split("/")[-1]}'

    LOGGER.info(f'AWS lambda - Moving file to final state "{state}" from "{key}" to "{new_key}" with state')

    copy_object_in_s3(s3_client, bucket, key, new_key)

    delete_file_from_s3(s3_client, bucket, key)


def parse_record(record: map) -> Tuple[str, str]:
    bucket = str(record['s3']['bucket']['name'])
    key = str(record['s3']['object']['key'])
    return bucket, key


def add_meta_columns(df: pd.DataFrame, key: str) -> pd.DataFrame:
    file_name = key.split("/")[-1]

    matches = re.search(r'(\d{2}_\d{2}_\d{4})', file_name)

    if not matches:
        raise Exception('Failed to fetch the date from the file name.'
                        ' Are you sure it has the DD_MM_YYYY date format in it?')

    date_text = matches.group(1)

    file_date = datetime.strptime(date_text, '%d_%m_%Y')

    local_df = df.copy()
    local_df['file_name'] = file_name
    local_df['file_date'] = file_date
    local_df['processing_date'] = datetime.today().date()

    return local_df


def create_unique_id() -> str:
    return ''.join(random.sample(string.ascii_lowercase + string.digits, 15))


def get_file_date(key: str,
                  string_format: str,
                  date_format: str) -> date:

    matches = re.search(string_format, key.split('/')[-1])

    if not matches:
        raise Exception('Failed to fetch the date from the file name.'
                        f' Are you sure it has the {string_format} date format in it?')

    date_text = matches.group(0)

    return datetime.strptime(date_text, date_format).date()
