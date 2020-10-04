from source import Mysql
import pandas as pd

#dataframes
treino = pd.read_csv('../data/telecom_treino.csv')


sql_treino = """
CREATE TABLE treino_raw (
state                   varchar(10),
account_length          int,
area_code               varchar(25),
international_plan      varchar(10),
voice_mail_plan         varchar(10),
number_vmail_messages   int,
total_day_minutes       float,
total_day_calls         int,
total_day_charge        float,
total_eve_minutes       float,
total_eve_calls         int,
total_eve_charge        float,
total_night_minutes     float,
total_night_calls       int,
total_night_charge      float,
total_intl_minutes      float,
total_intl_calls        int,
total_intl_charge       float,
number_customer_service_calls int,
churn varchar(10) )"""



#Instanciando o BD
bd = Mysql()


#Criando as tabelas
bd.create_table(query=sql_treino,user='brunods', password='Bruno2208', host='127.0.0.1',database='brunods')

#Inserindo Dados
bd.insert_data(df=treino, tabela=' treino_raw', user='brunods', password='Bruno2208', host='127.0.0.1',database='brunods')