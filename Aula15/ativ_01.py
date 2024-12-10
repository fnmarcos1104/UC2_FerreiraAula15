# import pandas as pd
import polars as pl
from datetime import datetime


ENDERECO_DADOS = r'./Dados/'
 
try:
    print('Obtendo dados...')
 
    hora_inicio = datetime.now()
 
    df_fevereiro = pl.read_csv(ENDERECO_DADOS + '202402_NovoBolsaFamilia.csv', separator=';', encoding='iso-8859-1')
 
    print(df_fevereiro.head())
 
    hora_fim = datetime.now()
 
    print(f'Tempo de execução: {hora_fim - hora_inicio}')
 
except ImportError as e:  
    print('Erro ao obter dados', e)