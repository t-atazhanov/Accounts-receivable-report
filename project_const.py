#!/usr/bin/env python
# coding: utf-8

# In[ ]:
from sqlalchemy import create_engine

PATH = "C:/Users/atm/Desktop/data/from VJI/"
# ------------
FILE_NAME_dict = {'big': 'big_upload', 'raw': "raw_upload", 'date': "date_upload"}
# ------------
FILE_codes = "База данных для кодов клиента"
FILE_codes_upd = 'База данных для кодов клиента_обновление'
FILE_ZPART = "ZPART_BI_REP"

DB_PASSWORD = "zXbkfdtr10-nmgk"
engine = create_engine(f'postgresql+psycopg2://postgres:{DB_PASSWORD}@localhost:5432/AR')
DB_TABLE_NAME = "sap_report_ar"


# In[ ]:



# переменные для основного DataFrame
AR_columns_date = ["Контрагент", "Документ", "Дата", "Сумма", "Валюта", "Дата погашения", 
                   "Дни просрочки САП", "Итого дебиторская задолженность", "Просроченная дебиторская задолженность САП", 
                   "Плановая дебиторская задолженность", "Сектор", "Дата формирования отчета"]
# ----------

nrows = None
clients_to_skip = ['АО "НМЖК"', 'АО ТД "НМЖК"', 'ИП Варламова Е.Л.', 'ИП Варламова Елена Леонидовна',
                  'ООО "Свит Лайф Фудсервис"', 'OBA MARKET OOO', 'ООО "Главмолснаб"', 'ТОО "Сауда-Интер"',
                  'ООО "Восток-Запад"', 'ООО "Три-С Фуд Ритейл"', 'АМК ТОО', 'ТОО "Aspan Asia"', 
                   'ТОО "РегионПромСнаб"', 'АО "АрселорМиттал Темиртау"', 'ИП Лидер-Трэйд', 'ООО КАРВАН 2002']

commersants = ['Букаев Алексей Евгеньевич', 'Вылегженин Илья Леонидович', 'Гурова Елена Владимировна', 
               'Зайцев Александр Николаевич', 'Кабанов Вячеслав Юрьевич', 'Клячин Евгений Александрович', 
               'Кожемякин Иван Анатольевич', 'Кондратьев Максим Владимирович', 'Корнев Роман Александрович', 
               'Кутузова Ирина Геннадьевна', 'Ливенцев Сергей Владимирович', 'Митрофанов Григорий Николаевич', 
               'Мосюров Сергей Николаевич', 'Новиков Юрий Валерьевич', 'Чурилов Михаил Юрьевич', 
               'Кутлуев Эдуард Фанилевич']


# переменные для DataFrame с кодами ГП
CODES_columns = ["Наименование контрагента", "№ счета-фактуры", "Грузополучатель"]

# переменные для ZPART_BI_REP DataFrame
ZPART_columns = ["ID_GRUZOPOL", "HOLDING", "REGION_B2C", "COMMERSANT_B2C", "CHANNEL"]

# регионы и директора

directors = {'ВОЛГА ВЕРХНЯЯ': 'DISTR /Вылегженин', 'ВОЛГА НИЖНЯЯ': 'DISTR /Вылегженин', 
             'ВОСТОК': 'DISTR /Вылегженин', 'МОСКВА': 'DISTR /Кротова', 'СЕВЕРО-ЗАПАД': 'DISTR /Кротова', 
             'УРАЛ': 'DISTR /Вылегженин', 'ЦЕНТР': 'DISTR /Кротова','ЮГ': 'DISTR /Кротова'}


final_columns = ["Контрагент", "Документ", "Дата", "Сумма", "Валюта", "Дата погашения", 
                   "Дни просрочки САП", "Итого дебиторская задолженность", 
                   "Просроченная дебиторская задолженность САП", "Плановая дебиторская задолженность", 
                   "Дата формирования отчета", "Код грузополучателя", "Холдинг", "Бизнес регион", 
                   "Коммерсант", "Канал", "Директор", "Дни просрочки", "Итого просроченная задолженность"]
backup_renaming = {'backup.csv': 'backup_-1.csv', 'backup_-1.csv': 'backup_-2.csv', 'new_backup.csv': 'backup.csv'}

