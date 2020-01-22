import pyodbc
import sys
import datetime


#Работа с источником
class SourceDB:

 #Вывод свойств подключения для конкретного источника
 @staticmethod
 def src_db_cnct_parameters(source_id):


     error_code = "SourceNonExistent"

     src_qr = "SELECT " \
                 "src.SERVER_NAME, " \
                 "src.DATABASE_NAME, " \
                 "src.PORT_NUMBER, " \
                 "src.LOGIN, " \
                 "src.PASSWORD, " \
                 "srctp.DRIVER_NAME, " \
                 "tds.TDS_VERSION, " \
                 "srctp.SOURCE_TYPE_ID, " \
                 "src.SOURCE_NAME " \
                 "FROM unicorn_metadata.SOURCES src " \
                 "INNER JOIN unicorn_metadata.SOURCE_TYPE srctp " \
                 "on src.SOURCE_TYPE_ID=srctp.SOURCE_TYPE_ID " \
                 "CROSS JOIN unicorn_metadata.TDS_VERSION tds " \
                 "WHERE src.source_id="+str(source_id)

     src_error = UnicornMetadata.error_message(error_code) Пs

     src_qr_result = UnicornMetadata.unicorn_crsr.execute(src_qr).fetchone()


     if src_qr_result != None:
        cnct_parameters = {
                         'server_name':src_qr_result[0],
                         'database_name':src_qr_result[1],
                         'port_number':src_qr_result[2],
                         'login':src_qr_result[3],
                         'password':src_qr_result[4],
                         'driver': src_qr_result[5],
                         'tds_version': src_qr_result[6],
                         'source_type_id': src_qr_result[7],
                         'source_name': src_qr_result[8],
                         'binary_result': 1
                       }
     else: cnct_parameters = {'binary_result': 0, 'text_result': src_error}

     return cnct_parameters

 #проверка подключения к источнику
 @staticmethod
 def src_db_check_cnct(server_name, database_name, login, password, port_number, tds_version, driver):

     error_code = "ConnectionError"
     success_code = "ConnectionSuccess"

     error_txt = UnicornMetadata.error_message(error_code)
     success_txt = UnicornMetadata.error_message(success_code)

     error_result = {'binary_result': 0, 'text_result': error_txt}
     successful_result = {'binary_result': 1, 'text_result': success_txt}

     try:
      src_check_cnct = pyodbc.connect(
          server=server_name,
          database=database_name,
          user=login,
          tds_version=tds_version,
          password=password,
          port=port_number,
          driver=driver
      )
     except Exception:
         return error_result
     else: return successful_result

 #просмотр всех неудаленных источников
 @staticmethod
 def src_db_cnct_view():

     un_crsr = UnicornMetadata.unicorn_crsr

     src_view_qr = "SELECT " \
                   "src.SOURCE_ID, " \
                   "src.SOURCE_NAME, " \
                   "src.SOURCE_TYPE_ID, " \
                   "src_tp.SOURCE_TYPE_NAME " \
                   "FROM unicorn_metadata.SOURCES src " \
                   "LEFT JOIN unicorn_metadata.SOURCE_TYPE src_tp " \
                   "ON src.SOURCE_TYPE_ID=src_tp.SOURCE_TYPE_ID " \
                   "WHERE src.DISABLE_FLG='0'"

     src_view = un_crsr.execute(src_view_qr).fetchall()

     return src_view

 #создание подключения к источнику
 @staticmethod
 def src_db_cnct_create(source_type_nm, server_name, source_name, database, user, password, port, user_id):

     successful_create = "SuccessfulSourceCreate"
     unsuccessful_create = "UnsuccessfulSourceCreate"
     result = {"binary_result":-1,"text_result":"Default"}

     un_crsr = UnicornMetadata.unicorn_crsr

     #определяем source_type_id и driver_name
     if UnicornMetadata.src_type_driver(source_type_nm)["binary_result"] == 0:
         return UnicornMetadata.src_type_driver(source_type_nm)["value"]
     else:
         source_type_id = UnicornMetadata.src_type_driver(source_type_nm)["source_type_id"]
         driver_name = UnicornMetadata.src_type_driver(source_type_nm)["driver_name"]

     #определяем tds_version
     if UnicornMetadata.tds_version()["binary_result"] == 0:
         return UnicornMetadata.tds_version()["value"]
     else: tds_version = UnicornMetadata.tds_version()["value"]

     #проверяем параметры подключения
     check_cnct = SourceDB.src_db_check_cnct(server_name, database, user, password, port, tds_version[0], driver_name)
     if check_cnct["binary_result"] == 0:
         return check_cnct

     #проверка наименования источника
     if UnicornMetadata.src_nm_check(source_name)["binary_result"]==0:
         return UnicornMetadata.src_nm_check(source_name)["text_result"]

     #проверка на уже существующий источник
     if UnicornMetadata.src_server_db_check(server_name, database)["binary_result"] == 0:
         return UnicornMetadata.src_server_db_check(server_name, database)["text_result"]

     dttm = str(datetime.datetime.now())[0:19] #дата и время

     #если все хорошо, то вставляем новый источник в метаданные
     insert_src_qr = "INSERT INTO unicorn_metadata.SOURCES " \
                     "(SOURCE_TYPE_ID, SERVER_NAME, PORT_NUMBER, DATABASE_NAME, LOGIN, PASSWORD, CREATOR_ID, MODIFIED_ID, LAST_UPDATED, DISABLE_FLG)" \
                     " VALUES " \
                     "("+str(source_type_id)+",'"+server_name+"',"+str(port)+",'"+database+"','"+user+"','"+password+"',"+str(user_id)+","+str(user_id)+",'"+dttm+"','0')"

     try:
         un_crsr.execute(insert_src_qr)
         un_crsr.commit()
     except:
         un_crsr.rollback()

     #проверяем вставку в бд
     insert_check = un_crsr.execute("SELECT COUNT(*) FROM unicorn_metadata.SOURCES WHERE SERVER_NAME='"+server_name+"' AND DATABASE_NAME='"+database+"'").fetchone()
     if insert_check[0] != 0:
         result = {"binary_result": 1, "text_result": UnicornMetadata.error_message(successful_create)}
         return result
     else:
         result = {"binary_result":0, "text_result":UnicornMetadata.error_message(unsuccessful_create)}
         return result

 #изменение параметров источника
 @staticmethod
 def src_db_cnct_update(source_id, user_id, source_name=None, server_name=None, database=None, login=None, password=None, port=None):

     #если атрибут не изменяется пользователем, то оно передается пустое в метод
     #определение изменившегося атрибута на стороне клиента
     #как минимум один передаваемый атрибут должен быть не пустой!

     result = {"binary_result": -1, "text_result": "Default"}
     modified_id = ", MODIFIED_ID="+str(user_id)
     dttm = str( datetime.datetime.now() )[0:19]  # дата и время
     dttm_qr = ", LAST_UPDATED='"+dttm+"'"

     success_update = "SuccessfulSourceUpdate"
     unsuccess_update = "UnsuccessfulSourceUpdate"


     #вытаскиваем текущие параметры источника
     src_prm = SourceDB.src_db_cnct_parameters(source_id)
     if src_prm["binary_result"] == 0:
         return src_prm["text_result"]


     #проставляем NULL пустым переменным
     if source_name == None:
         source_name = src_prm["source_name"]
         source_name_qr = " SOURCE_NAME = SOURCE_NAME" #требуется для начала после SET
     else:
         source_name_qr = " SOURCE_NAME='"+source_name+"'"
         # проверка наименования источника
         if UnicornMetadata.src_nm_check( source_name )["binary_result"] == 0:
             return UnicornMetadata.src_nm_check( source_name )["text_result"]

     if server_name == None:
         server_flg = 0
         server_name = src_prm["server_name"]
         server_name_qr=""
     else:
         server_name_qr = ", SERVER_NAME='"+server_name+"'"
         server_flg = 1

     if database == None:
         database_flg = 0
         database = src_prm["database_name"]
         database_name_qr=""
     else:
         database_name_qr = ", DATABASE_NAME='"+database+"'"
         database_flg = 1

     if login == None:
         login = src_prm["login"]
         login_qr=""
     else:
         login_qr = ", LOGIN='"+login+"'"

     if password == None:
         password = src_prm["password"]
         password_qr=""
     else:
         password_qr = ", PASSWORD='" + password + "'"

     if port == None:
         port = src_prm["port_number"]
         port_qr=""
     else:
         port_qr = ", PORT_NUMBER='" + port + "'"

     tds_version = UnicornMetadata.tds_version()["value"]

     un_crsr = UnicornMetadata.unicorn_crsr

     # проверка на уже существующий источник
     if server_flg == 1 or database_flg == 1:
         if UnicornMetadata.src_server_db_check( server_name, database )["binary_result"] == 0:
             return UnicornMetadata.src_server_db_check(server_name, database)["text_result"]

     #проверяем параметры подключения
     check_cnct = SourceDB.src_db_check_cnct(server_name, database, login, password, port, tds_version,src_prm["driver"])
     if check_cnct["binary_result"] == 0:
         return check_cnct

     #если все ок, изменяем параметры
     update_src_qr = "UPDATE unicorn_metadata.SOURCES" \
                     " SET"+source_name_qr+server_name_qr+database_name_qr+login_qr+password_qr+port_qr+modified_id+dttm_qr+ \
                     " WHERE SOURCE_ID="+str(source_id)

     try:
         un_crsr.execute(update_src_qr)
         un_crsr.commit()
     except:
         un_crsr.rollback()

     #проверяем, что обновилось корректно
     update_check = un_crsr.execute("SELECT SOURCE_NAME, SERVER_NAME, DATABASE_NAME, LOGIN, PASSWORD, PORT_NUMBER FROM unicorn_metadata.SOURCES WHERE SOURCE_ID="+str(source_id)).fetchone()

     if update_check == None:
         result = {"binary_result": 0, "text_result":UnicornMetadata.error_message(unsuccess_update)}
         return result

     if update_check[0] != source_name or update_check[1] != server_name or update_check[2] != database or update_check[3] != login or update_check[4] != password or update_check[5] != port:
         result = {"binary_result": 0, "text_result": UnicornMetadata.error_message( unsuccess_update )}
         return result

     result = {"binary_result": 1, "text_result": UnicornMetadata.error_message(success_update)}
     return result

 #удаление источника
 @staticmethod
 def src_db_cnct_delete(source_id, user_id):

     result = {"binary_result": -1, "text_result": "Default"}
     unsuccess_delete = "UnsuccessfulSourceDelete"
     success_delete = "SuccessfulSourceDelete"

     #проверяем, что такой источник существует
     src_prm = SourceDB.src_db_cnct_parameters( source_id )
     if src_prm["binary_result"] == 0:
         return src_prm["text_result"]

     dttm = str( datetime.datetime.now() )[0:19]  # дата и время

      #проставление DISABLE_FLG
     delete_qr = "UPDATE unicorn_metadata.SOURCES" \
                 " SET DISABLE_FLG='1', MODIFIED_ID="+str(user_id)+", LAST_UPDATED='"+dttm+"'" \
                 " WHERE SOURCE_ID="+str(source_id)

     un_crsr = UnicornMetadata.unicorn_crsr

     try:
         un_crsr.execute(delete_qr)
         un_crsr.commit()
     except:
         un_crsr.rollback()

     #проверяем удаление
     src_check = un_crsr.execute("SELECT DISABLE_FLG FROM unicorn_metadata.SOURCES WHERE SOURCE_ID="+str(source_id)).fetchone()
     if src_check[0] == 1:
         result = {"binary_result": 1, "text_result": UnicornMetadata.error_message(success_delete)}
     else:
         result = {"binary_result": 0, "text_result": UnicornMetadata.error_message(unsuccess_delete)}

     return result






 #подключение к источнику
 @staticmethod
 def src_db_cnct(server_name, database_name, login, password, port_number, tds_version, driver):

     if SourceDB.src_db_check_cnct(server_name, database_name, login, password, port_number, tds_version, driver)['binary_result'] == 1:
         src_crsr = pyodbc.connect(
             server=server_name,
             database=database_name,
             user=login,
             tds_version=tds_version,
             password=password,
             port=port_number,
             driver=driver
         )
     else: return SourceDB.src_db_check_cnct(server_name, database_name, login, password, port_number, tds_version, driver)['text_result']

     return src_crsr

#просмотр схем источника
 @staticmethod
 def src_db_schema_view(source_id):

     #Добавить исключение технических схем

     #Инициализация тех. переменных
     result = {"binary_result": -1, "value":"Default"}
     schema_value = 'Schema'
     table_value = 'Schemas'
     schema_name_value = 'Schemas_SchemaName'

     src_prm = SourceDB.src_db_cnct_parameters(source_id)

     #Проверка подключения к источнику
     if src_prm['binary_result'] == 0:
         return src_prm['text_result']

     #Схема метеданных источника
     schema_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], schema_value )
     if schema_qr["binary_result"] == 1:
         schema = schema_qr["value"]
     else:
         result = schema_qr


     #Схема метеданных источника
     table_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], table_value )
     if table_qr["binary_result"] == 1:
         table = table_qr["value"]
     else:
         result = table_qr

     #Схема метеданных источника
     schema_name_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], schema_name_value )
     if schema_name_qr["binary_result"] == 1:
         schema_name = schema_name_qr["value"]
     else:
         result = schema_name_qr

     if result["binary_result"] == 0:
         return result["value"]

     # Подключение к источнику
     src_crsr = SourceDB.src_db_cnct(
         src_prm['server_name'],
         src_prm['database_name'],
         src_prm['login'],
         src_prm['password'],
         src_prm['port_number'],
         src_prm['tds_version'],
         src_prm['driver']
     ).cursor()

     #Выбор всех схем
     src_schemas = src_crsr.execute("SELECT " + schema_name + " FROM " + schema + "." + table).fetchall()

     if src_schemas.__len__() != 0:
         return src_schemas
     else:
         return UnicornMetadata.error_message("NoneSourceSchemas")


 #просмотр объектов источника соответствующей схемы
 @staticmethod
 def src_db_obj_view(source_id, table_schema_val):

     #Инициализация тех. переменных
     result = {"binary_result": -1, "value":"Default"}
     empty_src_error = "NoneSourceTables"
     schema_value = 'Schema'
     tables_value = 'Tables'
     table_schema_value = 'Tables_TableSchema'
     table_name_value = 'Tables_TablesName'
     table_type_value = 'Tables_TableType'

     src_prm = SourceDB.src_db_cnct_parameters(source_id)

     #Проверка подключения к источнику
     if src_prm['binary_result'] == 0:
         return src_prm['text_result']

     #Схема метеданных источника
     schema_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], schema_value)
     if schema_qr["binary_result"]==1:
         schema = schema_qr["value"]
     else:
         result = schema_qr

     #Объект с таблицами источника
     tables_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], tables_value )
     if tables_qr["binary_result"] == 1:
         tables = tables_qr["value"]
     else:
         result = tables_qr

     #Схема объекта источника
     table_schema_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], table_schema_value )
     if table_schema_qr["binary_result"] == 1:
         table_schema = table_schema_qr["value"]
     else:
         result = table_schema_qr

     #Объект источника
     table_name_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], table_name_value )
     if table_name_qr["binary_result"] == 1:
         table_name = table_name_qr["value"]
     else:
         result = table_name_qr

     #Тип объекта источника
     table_type_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], table_type_value )
     if table_type_qr["binary_result"] == 1:
         table_type = table_type_qr["value"]
     else:
         result = table_type_qr


     if result["binary_result"] ==0:
         return result["value"]

     #Подключение к источнику
     src_crsr = SourceDB.src_db_cnct(
         src_prm['server_name'],
         src_prm['database_name'],
         src_prm['login'],
         src_prm['password'],
         src_prm['port_number'],
         src_prm['tds_version'],
         src_prm['driver']
     ).cursor()

     #Выбор всех таблиц из схемы
     src_tables = src_crsr.execute("SELECT " + table_name + ", " + table_type + " FROM " + schema + "." + tables + " WHERE "+table_schema+"='"+table_schema_val+"'").fetchall()

     if src_tables.__len__() != 0:
         return src_tables
     else:
         return UnicornMetadata.error_message(empty_src_error)

 #просмотр атрибутов таблицы
 @staticmethod
 def src_db_attr_view(source_id, schema_val, table_val):

     # Инициализация тех. переменных
     result = {"binary_result": -1, "value": "Default"}
     empty_src_error = "NoneSourceAttr"
     schema_value = 'Schema'
     columns_value = 'Columns'
     column_schema_value = 'Tables_ColumnsSchema'
     column_name_value = 'Tables_ColumnsName'
     column_table_value = 'Tables_ColumnsTable'

     src_prm = SourceDB.src_db_cnct_parameters(source_id)

     # Проверка подключения к источнику
     if src_prm['binary_result'] == 0:
         return src_prm['text_result']

     #Схема метеданных источника
     schema_qr = UnicornMetadata.source_meta( src_prm["source_type_id"], schema_value )
     if schema_qr["binary_result"] == 1:
         schema = schema_qr["value"]
     else:
         result = schema_qr

     #Таблица атрибутов источника
     columns_qr = UnicornMetadata.source_meta(src_prm["source_type_id"], columns_value )
     if columns_qr["binary_result"] == 1:
         columns = columns_qr["value"]
     else:
         result = columns_qr

     #Схема объекта таблицы атрибутов источника
     column_schema_qr = UnicornMetadata.source_meta( src_prm["source_type_id"], column_schema_value )
     if column_schema_qr["binary_result"] == 1:
         column_schema = column_schema_qr["value"]
     else:
         result = column_schema_qr

     #Атрибута источника
     column_name_qr = UnicornMetadata.source_meta( src_prm["source_type_id"], column_name_value )
     if column_name_qr["binary_result"] == 1:
         column_name = column_name_qr["value"]
     else:
         result = column_name_qr

     #Таблица атрибута источника
     column_table_qr = UnicornMetadata.source_meta( src_prm["source_type_id"], column_table_value )
     if column_table_qr["binary_result"] == 1:
         column_table = column_table_qr["value"]
     else:
         result = column_table_qr

     if result["binary_result"] ==0:
         return result["value"]

     #Подключение к источнику
     src_crsr = SourceDB.src_db_cnct(
         src_prm['server_name'],
         src_prm['database_name'],
         src_prm['login'],
         src_prm['password'],
         src_prm['port_number'],
         src_prm['tds_version'],
         src_prm['driver']
     ).cursor()

     #Выбор всех атрибутов из схемы
     src_columns = src_crsr.execute("SELECT " + column_name+" FROM " + schema + "." + columns + " WHERE " + column_schema + "='" + schema_val + "' AND TABLE_NAME='"+table_val+"'").fetchall()

     if src_columns.__len__() != 0:
         return src_columns
     else:
         return UnicornMetadata.error_message(empty_src_error)

 #Предросмотр данных таблицы источника
 @staticmethod
 def src_db_data_view(source_id, schema, table):

     #Инициализация тех. переменных
     smth_wrong = "SomethinGoneWrong"

     src_prm = SourceDB.src_db_cnct_parameters(source_id )

     #Подключение к источнику
     src_crsr = SourceDB.src_db_cnct(
         src_prm['server_name'],
         src_prm['database_name'],
         src_prm['login'],
         src_prm['password'],
         src_prm['port_number'],
         src_prm['tds_version'],
         src_prm['driver']
     ).cursor()

     qr = "SELECT TOP 100 * FROM " + schema + "." + table

     result = src_crsr.execute( qr ).fetchall()

     if result.__len__() != None:
         return result
     else: return UnicornMetadata.error_message(smth_wrong)