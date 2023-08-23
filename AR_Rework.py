#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# импорт библиотек
import numpy as np
import pandas as pd
import psycopg2, time
from sqlalchemy import create_engine

import project_const as const
import sql_functions


# In[ ]:


class Rework:

    def __init__(self):
        self.t_start = time.time()
        try:
            sql_functions.export_base_to_csv() # попытка создать бэкап
        except Exception:
            print('Проблема, бэкап не создан')

        finally:
            self.engine = create_engine(f'postgresql+psycopg2://postgres:{const.DB_PASSWORD}@localhost:5432/AR')
            self.conn = self.engine.raw_connection()

            self.sql_query = f"SELECT * FROM {const.DB_TABLE_NAME}"
            self.date_cols = ["Дата", "Дата погашения", "Дата формирования отчета"] # столбцы с датами для корр-й работы ф-и
            self.db_df = pd.read_sql(self.sql_query, self.conn, parse_dates=self.date_cols).iloc[:,1:12] # выгрузка из БД
            
            self.db_df.columns = const.final_columns[0:11]
            self.conn.close()
            #к этому моменту в DF мы выгрузили текущую БД и дали ей финальные имена            
            
            self.db_df = self.db_df[(~self.db_df['Контрагент'].isin(const.clients_to_skip)) & (~self.db_df["Дата"].isna())]
            self.db_df["Док_клиент"] = self.db_df["Контрагент"].map(str).map(str.upper) + self.db_df["Документ"].map(str) # сцепка КА и кода документа
            self.client_code = pd.read_excel(const.PATH + const.FILE_codes + ".xlsx", sheet_name='Sheet1', header=0, 
                                          usecols=const.CODES_columns, nrows=None) # import
            self.client_code["Док_клиент"] = self.client_code["Наименование контрагента"].map(str).map(str.upper) + self.client_code["№ счета-фактуры"].map(str)
            self.client_code = self.client_code.loc[:, ["Док_клиент", "Грузополучатель"]].drop_duplicates("Док_клиент").set_index('Док_клиент')["Грузополучатель"]          
            
            self.db_df["Грузополучатель"] = self.db_df["Док_клиент"].map(self.client_code) # подтягивается ГП сцепкой из ориг. DF
            
            
            
            self.db_df['Грузополучатель'].mask((self.db_df["Грузополучатель"]!=self.db_df["Грузополучатель"]) & (self.db_df["Документ"]!=self.db_df["Документ"]) & (self.db_df["Контрагент"]=='АО "Торговый дом "Перекресток"'), 
                                               1900000310, inplace=True)
            
            self.db_df['Грузополучатель'].mask((self.db_df["Грузополучатель"]!=self.db_df["Грузополучатель"]) & (self.db_df["Документ"]!=self.db_df["Документ"]) & (self.db_df["Контрагент"]=='ООО "О`КЕЙ"'), 
                                               1900000293, inplace=True)            
 
            self.db_df['Грузополучатель'].mask((self.db_df["Грузополучатель"]!=self.db_df["Грузополучатель"]) & (self.db_df["Документ"]!=self.db_df["Документ"]) & (self.db_df["Контрагент"]=='ООО "Русский Стиль - 97"'), 
                                               1100001923, inplace=True) 
            
            
            
            self.db_df.drop(columns=["Док_клиент"], inplace=True)
            self.zpart_bi_rep = pd.read_excel(const.PATH + const.FILE_ZPART + ".xlsx", sheet_name='Sheet1', header=0, 
                                              usecols=const.ZPART_columns, nrows=None)
            self.db_df = self.db_df.merge(self.zpart_bi_rep.drop_duplicates(), how='left', left_on="Грузополучатель", 
                                        right_on="ID_GRUZOPOL").drop(columns=["ID_GRUZOPOL"])

                    # заполняет столбец "директор"
            self.db_df['Директор'] = self.db_df.apply(lambda row: const.directors[row['REGION_B2C']] if row['CHANNEL'] == 'DISTR' 
                                                        else row['CHANNEL'], axis=1)

            # высчитываем корректные дни просрочки и сумму просроченной задолженности, сначала сделаем копии ориг-х столбцов
            self.db_df['Дни просрочки'] = self.db_df['Дни просрочки САП'].copy()
            self.db_df['Просроченная дебиторская задолженность'] = self.db_df['Просроченная дебиторская задолженность САП'].copy()
            self.db_df = self.db_df.apply(self.outside_overdue, axis=1) # конечное число дней и суммы (дебит-й) просрочки

            self.db_df.columns = const.final_columns # переименовали столбцы
            self.db_df["Дата"] = self.db_df["Дата"].map(pd.to_datetime)
            self.db_df["Дата погашения"] = self.db_df["Дата погашения"].map(pd.to_datetime)
            self.db_df["Дата формирования отчета"] = self.db_df["Дата формирования отчета"].map(pd.to_datetime)

            sql_functions.sql_table_drop()
            sql_functions.sql_table_create()
            
            print('начинаю обогащать БД')

            self.conn = self.engine.raw_connection()
            
            self.db_df.to_sql(f'{const.DB_TABLE_NAME}', self.engine, if_exists='append', index=False)
            self.conn.close()

            print("Конструктор класса отработал за:", round(time.time()-self.t_start), "секунд")

    def outside_overdue(self, row):
        """ отриц-е дни просрочки равняет 0; для НКА просрочка < 4 дней допустима (=0), для ост-о вернуть данные САП"""
        if row['Дни просрочки САП'] < 1:
            row["Дни просрочки"] = np.nan
        else:
            if row['CHANNEL'] == "NKA" and row['Дни просрочки САП'] <= 4:
                row["Дни просрочки"] = np.nan
                row["Просроченная дебиторская задолженность"] = 0
        return row
