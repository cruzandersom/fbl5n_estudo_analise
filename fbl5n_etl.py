import logging
import os
from typing import List, Tuple, Union
import pathlib
import sys
import boto3
import pandas as pd
import io
from unidecode import unidecode

sys.path.append(str(pathlib.Path(__file__).parent.absolute()))

from utils.database import insert_into_database, execute_query
from utils.helpers import create_parquet_key, create_parquet_and_send_to_s3, \
    parse_sap_format_to_number, read_file, move_file_to_final_state, add_meta_columns, create_unique_id, get_file_date

SYSTEM_NAME = 'sap'
DATABASE = 'fbl5n'
TABLE_NAME = 'fbl5n'

DATABASE_TABLE_NAME = 'fbl5n_stage'
TABLE_PRIMARY_KEY = 'key_unique_fbl5n'

NUMBER_COLUMNS = ['conta', 'mont_em_mi', 'datr', 'itm', 'conta_do_razao', 'are', 'doccompens']
DATE_COLUMNS = ['data_doc_', 'vencliquid', 'compensac_', 'data_base', 'entrado_em']

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, _):
    LOGGER.info('AWS lambda - FBL5N ETL execution started!')

    s3_client = boto3.client(
        's3',
        endpoint_url=os.environ['S3_ENDPOINT_URL']
    )

    for record in event['Records']:
        LOGGER.info(f'AWS lambda - FBL5N ETL - Processing Record: {record}')
        bucket, original_key, state = parse_record(record)

        try:
            file = read_file(s3_client, bucket, original_key)

            try:
                lines = file.decode('utf-8').split('\n')
            except UnicodeDecodeError:
                lines = file.decode('iso-8859-1').split('\n')

            df = parse_lines_to_dataframe(lines)

            LOGGER.info(f'AWS lambda - FBL5N ETL - Processing {len(df)} rows')

            structured_df = structure_dataframe(df)
            cleaned_df = clean_dataframe(structured_df)
            final_df = add_meta_columns(cleaned_df, original_key)

            insert_processed_dataframe_in_database(final_df)

            file_date = get_file_date(original_key, r'\d{2}_\d{2}_\d{4}', '%d_%m_%Y')

            parquet_key = create_parquet_key(file_date,
                                             SYSTEM_NAME,
                                             DATABASE,
                                             state)

            create_parquet_and_send_to_s3(s3_client, bucket, parquet_key, final_df)

            move_file_to_final_state(s3_client, bucket, SYSTEM_NAME, DATABASE, original_key, 'processed')

        except Exception as ex:
            LOGGER.error(f'AWS lambda - FBL5N ETL - Execution failed: {ex}')
            move_file_to_final_state(s3_client, bucket, SYSTEM_NAME, DATABASE, original_key, 'error')
            raise ex

    LOGGER.info('AWS lambda - FBL5N ETL - Execution finished!')


def add_unique_key(df: pd.DataFrame) -> pd.DataFrame:
    inside_df = df.copy()

    conta = df['conta'].astype(int).astype(str)
    document_number = df['no_doc_'].astype(int).astype(str)
    item = df['itm'].astype(int).astype(str)

    inside_df[TABLE_PRIMARY_KEY] = conta + '_' + document_number + '_' + item

    return inside_df


def parse_record(record: map) -> Tuple[str, str, str]:
    bucket = str(record['s3']['bucket']['name'])
    key = str(record['s3']['object']['key'])

    state = None

    if 'ABERTO' in key:
        state = 'receivables-debit'

    if 'COMPENSADO' in key:
        state = 'receivables-credit'

    if not state:
        raise Exception('File does not have a valid name. Please use COMPENSADO or ABERTO'
                        ' (e.g. CISP_COMPENSADO_16_12_2020_.txt)')

    return bucket, key, state


def parse_lines_to_dataframe(lines: List[str]) -> Union[pd.DataFrame, None]:
    header = None

    for row in lines:
        if row.startswith('|   St|'):
            header = row
            break

    if not header:
        raise Exception('Failed to find header row in the file')

    value_rows = [row.strip() for i, row in enumerate(lines) if row.startswith('| ') and not row.startswith('|   St|')]

    text_index = header.find('Texto')
    final_pipe_index = text_index + header[text_index:].find('|')

    for i, row in enumerate(value_rows):
        value_rows[i] = row[:text_index] + '"' + row[text_index:final_pipe_index] + '"' + row[final_pipe_index:]

    valid_rows = [header] + value_rows

    if len(valid_rows) == 0:
        return None

    with io.StringIO('\n'.join(valid_rows)) as text_io:
        df = pd.read_csv(text_io, sep='|', dtype={
            'Nº ID fiscal 1': str,
            'Nº doc.   ': str,
            'DocCompens': str,
            'Conta do Razão  ': str,
            'ChvRefer 3   ': str
        })

    return df.rename(str.strip, axis='columns')


def structure_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    LOGGER.info(f'AWS lambda - FBL5N ETL - Structuring DataFrame')

    valid_rows_df = df.copy()

    columns = list(map(convert_column_name, valid_rows_df.columns))

    valid_rows_df.columns = columns

    for column in NUMBER_COLUMNS:
        valid_rows_df[column] = valid_rows_df[column].astype(str) \
            .str.strip() \
            .str.replace('*', '') \
            .apply(parse_sap_format_to_number)

    for column in DATE_COLUMNS:
        valid_rows_df[column] = valid_rows_df[column].str.strip()
        valid_rows_df[column] = valid_rows_df[column].apply(lambda x: None if x == '' else x)
        valid_rows_df[column] = pd.to_datetime(valid_rows_df[column], format='%d.%m.%Y')

    valid_rows_df['no_id_fiscal_1'] = valid_rows_df['no_id_fiscal_1'].str.strip()

    def create_tipo_de_cliente_value(x: str):
        if not x:
            return None

        return 'CNPJ' if len(x) > 11 else 'CPF'

    valid_rows_df['tipo_de_cliente'] = \
        valid_rows_df['no_id_fiscal_1'].apply(create_tipo_de_cliente_value)

    return valid_rows_df.drop(['unnamed__0', 'unnamed__31'], axis='columns')


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    LOGGER.info('AWS lambda - FBL5N ETL - Cleaning Dataframe')

    valid_rows_df = filter_conta_values(df)
    valid_rows_df = filter_id_fiscal_values(valid_rows_df)
    valid_rows_df = filter_mont_em_mi_values(valid_rows_df)
    valid_rows_df = filter_texto_values(valid_rows_df)
    valid_rows_df = filter_data_doc_venc_liquid_values(valid_rows_df)
    valid_rows_df = filter_tipo_de_cliente_values(valid_rows_df)
    valid_rows_df = add_unique_key(valid_rows_df)
    valid_rows_df = filter_duplicated_rows(valid_rows_df)

    return valid_rows_df.reset_index(drop=True)


def filter_conta_values(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df.conta > 9999) & (~df.conta.isna())]


def filter_id_fiscal_values(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df.no_id_fiscal_1.isna()]


def filter_mont_em_mi_values(df: pd.DataFrame) -> pd.DataFrame:
    return df[((df.mont_em_mi >= 10) | ((df.tip.isin(['Y4 ', 'X4 ', 'DZ '])) & (df.mont_em_mi < 0)))]


def filter_texto_values(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df.texto.str.lower().str.contains('deudor')]


def filter_data_doc_venc_liquid_values(df: pd.DataFrame) -> pd.DataFrame:
    return df[df.vencliquid >= df.data_doc_]


def filter_tipo_de_cliente_values(df: pd.DataFrame) -> pd.DataFrame:
    return df[df.tipo_de_cliente == 'CNPJ']


def filter_duplicated_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values('data_doc_', ascending=False).drop_duplicates(subset=[TABLE_PRIMARY_KEY])


def convert_column_name(name: str) -> str:
    return unidecode(name) \
        .strip() \
        .lower() \
        .replace(' ', '_') \
        .replace('.', '_') \
        .replace('/', '') \
        .replace(':', '_')


def insert_processed_dataframe_in_database(df: pd.DataFrame) -> None:
    LOGGER.info(f'AWS lambda - FBL5N ETL - Structuring DataFrame')

    dummy_table = f'fbl5n_dummy_{create_unique_id()}'

    insert_into_database(df, dummy_table, 'replace')

    query = f'''
     BEGIN        
         MERGE {DATABASE_TABLE_NAME} AS target USING (
             SELECT * FROM {dummy_table} 
             WHERE 
                 doccompens is null
                 or cast(doccompens as varchar) not like '9%'
                 or (rtrim(tip) in ('Y4','X4', 'DZ') and mont_em_mi < 0)
         ) AS source
             ON 
                 target.key_unique_fbl5n = source.key_unique_fbl5n
         WHEN MATCHED AND source.file_date >= target.file_date
             THEN UPDATE SET {','.join([f'target.{column} = source.{column}' for column in df.columns
                                        if column != TABLE_PRIMARY_KEY])} 
         WHEN NOT MATCHED
             THEN INSERT ({','.join(df.columns)})
             VALUES ({','.join([f'source.{column}' for column in df.columns])});

         DROP TABLE {dummy_table};
     END
     '''

    execute_query(query)
