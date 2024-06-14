import wget
import os
import datetime
import pandas as pd


URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demandas_dos_consumidores_nip/"

YEAR = datetime.datetime.now().year

RAW_PATH = 'raw/'

SILVER_PATH = 'silver/'

FILE_NAME = f"""PDA_NIP_{YEAR}.csv"""

FILE_LIST = ['PDA_NIP_2011.csv',
             'PDA_NIP_2012.csv',
             'PDA_NIP_2013.csv',
             'PDA_NIP_2014.csv',
             'PDA_NIP_2015.csv',
             'PDA_NIP_2016.csv',
             'PDA_NIP_2017.csv',
             'PDA_NIP_2018.csv',
             'PDA_NIP_2019.csv',
             'PDA_NIP_2020.csv',
             'PDA_NIP_2021.csv',
             'PDA_NIP_2022.csv',
             'PDA_NIP_2023.csv']

def download_file(download_type) -> None:
    """

    Download file from URL previously defined.
    
    """

    if download_type == 'ACTUAL_YEAR':

        remove_old_file(RAW_PATH+FILE_NAME)

        wget.download(URL+FILE_NAME, RAW_PATH+FILE_NAME)
        

    elif download_type == 'PREVIOUSLY_YEARS':
        
        for file in FILE_LIST:

            remove_old_file(RAW_PATH+file)

            wget.download(URL+file, RAW_PATH+file)

    else:

        print("Command not recognized")

def read_file(file) -> pd.DataFrame:
    """

    Read .csv and return Dataframe
    
    """

    # Defining field types
    dtype = {"NUMERO_DA_DEMANDA" : 'int64',
         "ANO_DE_REFERENCIA" : 'int64',
         "MES_DE_REFERENCIA": 'int64',
         "CLASSIFICACAO_DA_NIP": 'string',
         "NATUREZA_DA_NIP": 'string',
         "CODIGO_OPERADORA": 'string',
         "NOME_OPERADORA": 'string'
       }

    # Reading file with pandas
    df = pd.read_csv(file, sep=';', on_bad_lines='skip', encoding='latin', dtype=dtype)

    # selecting just needed columns
    filtred_df = df[['NUMERO_DA_DEMANDA', 'ANO_DE_REFERENCIA', 'MES_DE_REFERENCIA', 'CLASSIFICACAO_DA_NIP', 'NATUREZA_DA_NIP','CODIGO_OPERADORA','NOME_OPERADORA']]
    
    return filtred_df

def write_parquet() -> None:
    """

    Create .parquet file
    
    """
    files = os.listdir(RAW_PATH)

    # Loop FILE_LIST to create .parquet to each file.
    for file in files:

        # Read file and return dataframe.
        df = read_file(RAW_PATH + file)

        # Remove extension file to create a .parquet name.
        file = file[:-4]

        # write .parquet.
        df.to_parquet(SILVER_PATH+file + '.parquet' ,engine='pyarrow', compression='snappy')

def remove_old_file(file):
    """

    Remove previously files
    
    """

    if os.path.exists(file):

        os.remove(file)

    return True


if __name__ == "__main__":

    # Download files, you can choice if you need download corrently year or previously years. Params : ['PREVIOUSLY_YEARS'] or ['ACTUAL_YEAR']
    download_file('PREVIOUSLY_YEARS')

    # Write .parquet file
    write_parquet()