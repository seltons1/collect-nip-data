import wget
import os
import datetime


URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demandas_dos_consumidores_nip/"

YEAR = datetime.datetime.now().year

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

def download_file(file):

    wget.download(URL, file)

    return True

if __name__ == "__main__":

    download_file(FILE_NAME)