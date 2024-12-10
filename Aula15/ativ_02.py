import pandas as pd
import polars as pl
from datetime import datetime
import os
import gc
   

ENDERECO_DADOS = r'./Dados/'

 
try:

 
    hora_inicio = datetime.now()

    lista_arquivos = ['202404_NovoBolsaFamilia.csv', '202405_NovoBolsaFamilia.csv']

    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso=8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df

        print(df.head())

    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA')
    )

    df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    del df

    gc.collect()

    hora_final = datetime.now()
    print(f'Tempo de execução: {hora_final - hora_inicio}')
 
except ImportError as e:  
    print('Erro ao obter dados', e)