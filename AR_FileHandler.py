#!/usr/bin/env python
# coding: utf-8

# In[1]:


# импорт библиотек
import numpy as np
import pandas as pd
import psycopg2, time
from sqlalchemy import create_engine

# from project_const import *
import project_const as const
import sql_functions


# In[2]:


class AR_Handler:
    def __init__(self, method):
        '''
        выбрать один из методов обработки файла: "raw", "big" or "date"
        '''
        t_begin = time.time()
        
#         -----------------------------------------------------------------------------------------------------
    
        # РАБОТА С ГЛАВНЫМ ФАЙЛОМ
        # определяем требуемое имя
        if method == 'date':
            FILE_NAME = const.FILE_NAME_dict['date']
        elif method == 'raw':
            FILE_NAME = const.FILE_NAME_dict['raw']

        self.df = pd.read_excel(const.PATH + FILE_NAME + ".xlsx", sheet_name="Основной", header=None, 
                                names=const.AR_columns_date, usecols="C:N", skiprows=8, nrows=const.nrows, 
                                dtype={"Документ": str}) # импорт файла
    
        self.__set_report_date()
        self.df['Чья задолженность'], self.df['Коммерсант'] = np.nan, np.nan
        self.tech_var = np.nan
        self.df = self.df.apply(self.__fill_commers, axis=1) # определяем ответственных коммерсантов
        self.df = self.df.apply(self.__b2c_accounts, axis=1) # определим строки, относящиеся к B2C
        self.df = self.df[self.df['Чья задолженность']=='B2C'].drop(columns=["Сектор", "Чья задолженность", "Коммерсант"])
        

            
        # отбрасывает ненужных контрагентов и строки без даты документа
        self.df = self.df[(~self.df['Контрагент'].isin(const.clients_to_skip)) & (~self.df["Дата"].isna())]
        

        self.df["Дата"] = self.df['Дата'].map(lambda date: self.__convert_excel_time(date) if type(date)==int                                               else date) # в поле "дата" могут быть числ. xls форматы, исправляет
    
        # сцепка КА и кода документа
        self.df["Док_клиент"] = self.df["Контрагент"].map(str).map(str.upper) + self.df["Документ"].map(str) 
        
#         -----------------------------------------------------------------------------------------------------

        # вспомог-й DF с кодами клиентов, будем его далее присоединять
        self.client_code = self.__client_code_updater() # обновление файла с кодами клиентов и сохранение в DF

        # сцепка КА и кода док-та, убираются дубликаты, чтобы одной связке соответствовал лишь один код ГП
        self.client_code["Док_клиент"] = self.client_code["Наименование контрагента"].map(str).map(str.upper) + self.client_code["№ счета-фактуры"].map(str)
        self.client_code = self.client_code.loc[:, ["Док_клиент", "Грузополучатель"]].\
        drop_duplicates("Док_клиент").set_index('Док_клиент')["Грузополучатель"]

        self.df["Грузополучатель"] = self.df["Док_клиент"].map(self.client_code) # подтягивается ГП сцепкой из ориг. DF
        self.df['Грузополучатель'].mask((self.df["Грузополучатель"]!=self.df["Грузополучатель"]) & (self.df["Документ"]!=self.df["Документ"]) & (self.df["Контрагент"]=='АО "Торговый дом "Перекресток"'), 
                                           1900000310, inplace=True)
        self.df.drop(columns=["Док_клиент"], inplace=True)
#         -----------------------------------------------------------------------------------------------------    
#         # выгружаем имеющиеся данные из БД, добавляем к ней наши новые данные, сохраняем
#         то есть в df_new у нас будут старые данные из БД + новые из файла, это и пойдет после всех манип-й в БД
        self.db_df = self.__get_db_df() # через функцию получить данные из БД
        self.df_new = pd.concat([self.df, self.db_df]) # объединить данные из БД и новые
    
#         -----------------------------------------------------------------------------------------------------        
#         # другой вспомог-й DF

        # import ZPART_BI_REP
        
        self.zpart_bi_rep = pd.read_excel(const.PATH + const.FILE_ZPART + ".xlsx", sheet_name='Sheet1', header=0, 
                                          usecols=const.ZPART_columns)
        
        
#         # создаём финальный DF на основе предыдущих, отбрасываем ненужные столцбы

        self.df_new = self.df_new.merge(self.zpart_bi_rep.drop_duplicates(), how='left', left_on="Грузополучатель", 
                                    right_on="ID_GRUZOPOL").drop(columns=["ID_GRUZOPOL"])

#         -----------------------------------------------------------------------------------------------------        
        # заполняет столбец "директор"
        self.df_new['Директор'] = self.df_new.apply(lambda row: const.directors[row['REGION_B2C']] if row['CHANNEL'] == 'DISTR' 
                                                    else row['CHANNEL'], axis=1)

        # высчитываем корректные дни просрочки и сумму просроченной задолженности, сначала сделаем копии ориг-х столбцов
        self.df_new['Дни просрочки'] = self.df_new['Дни просрочки САП'].copy()
        self.df_new['Просроченная дебиторская задолженность'] = self.df_new['Просроченная дебиторская задолженность САП'].copy()
        self.df_new = self.df_new.apply(self.__overdue, axis=1) # конечное число дней и суммы (дебит-й) просрочки
        self.df_new.columns = const.final_columns # переименовали столбцы

        t_end = time.time()
        print("Конструктор класса отработал за:", round(t_end - t_begin), "секунд")

        
#         ------------------------------------------------------------------------------------
#         ------------------------------------------------------------------------------------
#         ------------------------------------------------------------------------------------

    def __get_db_df(self):
        """вернуть имеющуюся БД"""
        engine = create_engine(f'postgresql+psycopg2://postgres:{const.DB_PASSWORD}@localhost:5432/AR')
        conn = engine.raw_connection()
        sql_query = f"SELECT * FROM {const.DB_TABLE_NAME}"
        date_cols = ["Дата", "Дата погашения", "Дата формирования отчета"] # столбцы с датами для корр-й работы ф-и
        try:
            db_df = pd.read_sql(sql_query, conn, parse_dates=date_cols).iloc[:, 1:-7] # выгрузка из БД
        except Exception:
            raise BaseException(f''''Ошибка при получении данных из таблицы {const.DB_TABLE_NAME}, программа прервана. 
                                Возможно, база данных не создана''')
        conn.close()
        db_df.columns = self.df.columns
        return db_df

    
    def __set_report_date(self):
        """правильно ставим дату формирования отчета в DF"""
        date_var = None
        if self.df.iloc[:,-1].dtype == '<M8[ns]':
            date_var = self.df.iloc[0:,-1]
        else:
            date_day, date_month, date_year = str(self.df.iloc[0,-1]).split('.')

        print(date_var)
        if date_var is not None:
            self.df["Дата формирования отчета"] = self.df.iloc[0:,-1]
            self.df["Дата формирования отчета"] = self.df["Дата формирования отчета"].map(pd.to_datetime)
        else:
            self.df["Дата формирования отчета"] = pd.Timestamp(year = int(date_year),month = int(date_month),                                                            day = int(date_day))
        
        
    def __overdue(self, row):
        """ отриц-е дни просрочки равняет 0; для НКА просрочка < 4 дней допустима (=0), для ост-о вернуть данные САП"""
        if row['Дни просрочки САП'] < 1:
            row["Дни просрочки"] = np.nan
        else:
            if row['CHANNEL'] == "NKA" and row['Дни просрочки САП'] <= 4:
                row["Дни просрочки"] = np.nan
                row["Просроченная дебиторская задолженность"] = 0
        return row

    
    def __fill_commers(self, row):
        ''' заполняет поле коммерсант '''
        if pd.isnull(row["Валюта"]):
            row['Коммерсант'] = row['Контрагент']
            self.tech_var = row['Контрагент']
        else:
            row['Коммерсант'] = self.tech_var
        return row
        
        
    def __b2c_accounts(self, row):
        """ определяет строки, относящиеся к B2C """
        if (np.isnan(row['Сектор']) and row['Контрагент'] == 'АО "Торговый дом "Перекресток"') or row['Коммерсант'] in const.commersants         or (row['Сектор'] == 2 and any(x in str(row['Коммерсант']) for x in ['ДИКСИ', 'МЕТРО', 'Перекресток'])):
            row['Чья задолженность'] = 'B2C'
        return row

    def __convert_excel_time(self, excel_time):
        ''' converts number of days since 1970.01.01 into date as it is in MS Excel  '''
        adj = 1 if excel_time < 60 else 2 # корректировка известного Эксель бага, что 1900 был висок-м г. - он не был
        return pd.to_datetime('1900-01-01') + pd.to_timedelta(excel_time - adj,'D')

            
    def __client_code_updater(self):
        """объединив старую базу и файл с новыми фактурами, удаляет дубликаты"""


        t0 = time.time()
        # импорты файлов
        client_code = pd.read_excel(const.PATH + const.FILE_codes + ".xlsx", sheet_name='Sheet1',
                                    header=0, usecols="A:K", nrows=None, dtype={"№ счета-фактуры": str})
        client_code_upd = pd.read_excel(const.PATH + const.FILE_codes_upd + ".xlsx", sheet_name='Sheet1', 
                                        header=0, usecols="A:K", nrows=None, dtype={"№ счета-фактуры": str})

        client_code = pd.concat([client_code, client_code_upd]).drop_duplicates()
        client_code.to_excel(const.PATH + const.FILE_codes + ".xlsx", sheet_name='Sheet1', index=False)
        t1 = time.time()
        print("файл с кодами клиентов обновлён за:", round(t1-t0), "секунд")
        return client_code
    
    
    def add_to_sql_data(self):
        '''загрузка на SQL сервер
        выдаёт ошибку, если какие-то строки пропали'''

        if (self.df.shape[0] + self.db_df.shape[0]) != (self.df_new.shape[0]):
            raise BaseException('Потеряно {} строк'.format(self.df_new.shape[0] - self.df.shape[0] + self.db_df.shape[0])
                               )
        else:
            sql_functions.export_base_to_csv()
            sql_functions.sql_table_create()

            conn = const.engine.raw_connection()
            self.df_new.to_sql(f'{const.DB_TABLE_NAME}', const.engine, if_exists='append', index=False)
            conn.close()

