import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from sklearn.base import BaseEstimator, TransformerMixin


class SelecionaVariaveis(TransformerMixin, BaseEstimator):

    def __init__(self, variaveis=None):
        if variaveis is None:
            variaveis = []
        self._variaveis = variaveis

    def fit(self, X,y=None):
        return self

    def transform(self, X, y=None):
        return X[self._variaveis]

class ToDenseTransformer(TransformerMixin, BaseEstimator):
    def fit(self, X, y=None, **fit_params):
        return self

    def transform(self, X, y=None, **fit_params):
        return X.todense()



class Mysql:

    def create_table(self, query, user, password, host, database, connect=True):

        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'raise_on_warnings': True
        }

        try:
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()
            create_query = query

            cursor.execute(create_query)
            print('Tabela criada no Mysql com sucesso')

        except mysql.connector.Error as err:
            print(f'Tabela não foi criada devido ao erro {err}')

        finally:

            if connect:
                cursor.close()
                cnx.close()
                print('Conexão fechada com o Mysql')


    def retrieve_data(self, query, user, password, host, database, objeto='pd', connect=True):

        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'raise_on_warnings': True
        }

        try:
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()
            create_query = query

            cursor.execute(create_query)
            #print('Buscando os dados!!!')

            dados = cursor.fetchall()
            colunas = [desc[0] for desc in cursor.description]

            if objeto == 'pd':
                base = pd.DataFrame(dados, columns=colunas)
            else:
                base = np.array(dados)
            return base

        except mysql.connector.Error as err:
            print(f'Erro durante a query {err}')

        finally:

            if connect:
                cursor.close()
                cnx.close()
                #print('Conexão fechada com o Mysql')


    def insert_data(self,df ,tabela , user, password, host, database, connect=True):


        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'raise_on_warnings': True
        }


        try:
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()

            cols = ",".join([str(i) for i in df.columns.tolist()])

            for _, row in df.iterrows():
                sql = "INSERT INTO" + tabela + "(" + cols + ") VALUES (" + "%s, "*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
                cnx.commit()

        except mysql.connector.Error as err:
            print(f'Erro durante a query {err}')

        finally:

            if connect:
                cursor.close()
                cnx.close()
                print('Conexão fechada com o Mysql')
