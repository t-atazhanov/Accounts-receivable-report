#!/usr/bin/env python
# coding: utf-8

import project_const as const
import psycopg2, os
import pandas as pd

# In[ ]:

def sql_table_drop():
    """ удаляет таблицу по задолженностям из БД, если она существует """
    try:
        # connect to db
        connection = psycopg2.connect(
            host="localhost", 
            user="postgres", 
            password=const.DB_PASSWORD, 
            database='AR'
        )
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                f"""DROP TABLE IF EXISTS {const.DB_TABLE_NAME}"""
            )
            print("[INFO] Table deleted")
    except Exception as ex:
        print(" [INFO] Error while working with PostgreSQL", ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

# In[ ]:


def sql_table_create():
    """ удаляет старую, а потом создаёт новую пустую таблицу с инфо по задолженностям в БД """
    sql_table_drop()
    try:
        # connect to db
        connection = psycopg2.connect(
            host="localhost", 
            user="postgres", 
            password=const.DB_PASSWORD, 
            database='AR'
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT version()"
            )
            print(f"Server version: {cursor.fetchone()}")

        with connection.cursor() as cursor:
    #         cursor.execute(
    #             f"""DROP TABLE IF EXISTS {const.DB_TABLE_NAME}"""
    #         )
    #         print("[INFO] Table deleted")

            cursor.execute(
                f"""CREATE TABLE IF NOT EXISTS {const.DB_TABLE_NAME}(
                id serial PRIMARY KEY,
                "Контрагент" VARCHAR(100),
                "Документ" VARCHAR(100),
                "Дата" DATE,
                "Сумма" NUMERIC,
                "Валюта" VARCHAR(100),
                "Дата погашения" DATE,
                "Дни просрочки САП" SMALLINT,
                "Итого дебиторская задолженность" NUMERIC,
                "Просроченная дебиторская задолженность САП" NUMERIC,
                "Плановая дебиторская задолженность" NUMERIC,
                "Дата формирования отчета" DATE,
                "Код грузополучателя" BIGINT,
                "Холдинг" VARCHAR(100),
                "Бизнес регион" VARCHAR(100),
                "Коммерсант" VARCHAR(100),
                "Канал" VARCHAR(100),
                "Директор" VARCHAR(100),
                "Дни просрочки" SMALLINT,
                "Итого просроченная задолженность" NUMERIC);
                """
            )
            print("[INFO] Table successfully created")
    except Exception as ex:
        print(" [INFO] Error while working with PostgreSQL", ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


# In[ ]:


def delete_by_date(date_to_delete):
    """ Удаляет из БД данные за выбранную дату """
    try:
        connection = psycopg2.connect(dbname='AR', user='postgres', password=const.DB_PASSWORD, host='localhost')
        connection.autocommit =  True

        with connection.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT DISTINCT "Дата формирования отчета" FROM {const.DB_TABLE_NAME}
            """
            )
            result = [el[0].strftime("%Y-%m-%d") for el in cursor.fetchall()]
            if date_to_delete in result:
                date_to_delete = str("'") + date_to_delete + str("'")
                cursor.execute(
                f"""
                DELETE FROM {const.DB_TABLE_NAME} WHERE "Дата формирования отчета" = {date_to_delete}
                """
                )
                print("[INFO] Data by period successfully deleted")
            else:
                print("[INFO] Date not found")
    except Exception as ex:
        print(" [INFO] Error while working with PostgreSQL", ex)
    finally:
        if connection:
            connection.close()


# In[ ]:


def export_base_to_csv():
    """скопировать основную таблицу из БД в .csv файл"""
    
    # create a query to specify which values we want from the database.
    query = f"SELECT * FROM {const.DB_TABLE_NAME}"

    # set up our database connection.
    conn = psycopg2.connect(dbname='AR', user='postgres', password=const.DB_PASSWORD, host='localhost')
    db_cursor = conn.cursor()

    # Use the COPY function on the SQL we created above.
    SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER ENCODING 'UTF-8'".format(query)

    # Set up a variable to store our file path and name.
    t_path_n_file = const.PATH + 'new_backup.csv'
    with open(t_path_n_file, 'w') as f_output:
        db_cursor.copy_expert(SQL_for_file_output, f_output)
    conn.close()
    
    
#     remove the oldest backup, change names of other backups
    try:
        with os.scandir(path=const.PATH) as inside_folder:
            files_inside = [entry.name for entry in inside_folder]
            if 'backup_-2.csv' in files_inside:
                os.remove(const.PATH+'backup_-2.csv')
            if 'backup_-1.csv' in files_inside:
                os.rename(src=const.PATH+'backup_-1.csv', dst=const.PATH+'backup_-2.csv')
            if 'backup.csv' in files_inside:
                os.rename(src=const.PATH+'backup.csv', dst=const.PATH+'backup_-1.csv')
                
            if 'new_backup.csv' not in files_inside:
                raise Exception('Бэкап не был создан')
            else:
                os.rename(src=const.PATH+'new_backup.csv', dst=const.PATH+'backup.csv')
    except:
        "Новый бэкап создан, но дальше произошла ошибка!"


# In[ ]:


def restore_db_from_csv():
    sql_table_drop()
    sql_table_create()
    dataframe = pd.read_csv(const.PATH+'backup.csv', usecols=[x for x in range(1,20)], encoding='cp1251', low_memory = False)
    dataframe.to_sql(f'{const.DB_TABLE_NAME}', const.engine, if_exists='append', index=False)
    


