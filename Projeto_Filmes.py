import pandas as pd
import numpy as np
from sqlalchemy import create_engine


#importar o arquivo
df = pd.read_csv('C:\\Users\\pablo.souza\\OneDrive - COMPASSO TECNOLOGIA LTDA\\Documentos\\Projeto_filmes_dados\\2122.csv', sep=',', encoding='cp1252', decimal =',')

#duplicando a coluna data de lançamento
df['Relançado'] = df.loc[:, 'Data de Lançamento']

#substituindo valores Relançamento e relançado por sim na coluna Relançado.
df = df.replace({'Relançado': {'relançamento' : 'Sim', 'Relançamento' : 'Sim'}})

#copiando novo DF
df_novo = df.copy()

#trocando datas por não.
df_novo.loc[(df.Relançado != 'Sim'),'Relançado']='Não'

#trocando Relançamento e relaçançamento da coluna data de lançamento por NaN
df_novo = df_novo.replace({'Data de Lançamento': {'relançamento' : np.nan, 'Relançamento' : np.nan}})

#converter a coluna de renda para float
df_novo['Renda (R$) na semana dos dados'] = df_novo ['Renda (R$) na semana dos dados'].str.replace('.','', regex=True)

df_novo['Renda (R$) na semana dos dados'] = df_novo ['Renda (R$) na semana dos dados'].str.replace(',','.', regex=True)

df_novo['Renda (R$) na semana dos dados'] = df_novo['Renda (R$) na semana dos dados'].astype(float)

#converter a coluna de publico na semana para float
df_novo['Público na semana dos dados'] = df_novo ['Público na semana dos dados'].str.replace('.','', regex=True)

df_novo['Público na semana dos dados'] = df_novo['Público na semana dos dados'].astype(float)

#converter a data de lançamento para data.
df_novo['Data de Lançamento'] = pd.to_datetime(df_novo['Data de Lançamento'])

#criar coluna Id para a tabela.
df_novo['id'] = df.index

#criar coluna Id para genero.
df_novo['id_genero'] = df[['Gênero']].sum(axis=1).map(hash)

#mudando a ordem das colunas
df_novo = df_novo[['id', 'Ano de exibição', 'Semana de exibição', 'CPB/ROE', 'Título da obra', 'id_genero', 'Gênero', 'País(es) produtor(es) da obra', 'Nacionalidade da obra', 'Data de Lançamento', 'Distribuidora', 'Origem da empresa distribuidora', 'Número de salas na semana dos dados', 'Público na semana dos dados', 'Renda (R$) na semana dos dados', 'Relançado']]

#criando dataframe filmes
df_filmes = df_novo[['id', 'Título da obra', 'id_genero', 'Nacionalidade da obra', 'País(es) produtor(es) da obra', 'Data de Lançamento', 'Distribuidora', 'Origem da empresa distribuidora']].copy()

#alterando o nome das colunas do df_filmes 
df_filmes.rename(columns={'Título da obra': 'titulo_obra', 'id_genero': 'id_genero', 'País(es) produtor(es) da obra': 'pais', 'Nacionalidade da obra': 'nacionalidade', 'Data de Lançamento': 'lancamento', 'Distribuidora': 'distribuidora', 'Origem da empresa distribuidora': 'origem_distribuidora'}, inplace = True)

#Criando df_Cinemas, 
df_cinema = df_novo[['id', 'Ano de exibição', 'Semana de exibição', 'Número de salas na semana dos dados', 'Público na semana dos dados', 'Renda (R$) na semana dos dados']].copy()

#alterando o nome das colunas do df_cinema 
df_cinema.rename(columns={'Ano de exibição': 'ano_exibicao', 'Semana de exibição': 'semana_exibicao', 'Número de salas na semana dos dados': 'salas_na_semana', 'Público na semana dos dados': 'publico_na_semana', 'Renda (R$) na semana dos dados': 'renda'}, inplace = True)

#Criando df_genero, 
df_genero = df_novo[['id_genero', 'Gênero',]].copy()

#alterando o nome das colunas do df_genero
df_genero.rename(columns={'Gênero': 'genero'}, inplace = True)

#remover valores duplicados do df_genero
df_genero = df_genero.drop_duplicates()

#Criando df_pais, 
df_pais = df_novo[['País(es) produtor(es) da obra',]].copy()

#alterando o nome das colunas do df_pais
df_pais.rename(columns={'País(es) produtor(es) da obra': 'pais'}, inplace = True)

#parte do banco

db_data = 'mysql+mysqldb://' + 'root' + ':' + 'root' + '@' + '172.21.48.1' + ':3306/' \
       + 'cinema' + '?charset=utf8mb4'
engine = create_engine(db_data)


# envia para o SQL os dados
df_pais.to_sql('pais', engine, if_exists='append', index=False)
df_genero.to_sql('genero', engine, if_exists='append', index=False)
df_filmes.to_sql('filmes', engine, if_exists='append', index=False)
df_cinema.to_sql('sala', engine, if_exists='append', index=False)

