# coding=utf-8
import pyodbc
import sys
import datetime

#Работа с метаданными
class UnicornMetadata:

    # Переменная подключения к метаданным Unicorn
    unicorn_server = "unicornsrc.cq4yw6qxxgcs.us-east-2.rds.amazonaws.com"
    unicorn_database = "unicorn_src"
    unicorn_user = "unicorn"
    unicorn_tds_version = "7.4"
    unicorn_password = "Unicorn19"
    unicorn_port = 1433
    unicorn_driver = "/usr/local/lib/libtdsodbc.so"
    unicorn_cnct = pyodbc.connect(
        server=unicorn_server,
        database=unicorn_database,
        user=unicorn_user,
        tds_version=unicorn_tds_version,
        password=unicorn_password,
        port=unicorn_port,
        driver=unicorn_driver
    )

    unicorn_crsr = unicorn_cnct.cursor()

    #Системные сообщения
    @staticmethod
    def error_message(message_code):

        message_qr = "SELECT MESSAGE_TEXT FROM unicorn_metadata.SYSTEM_MESSAGES WHERE MESSAGE_CODE='"+message_code+"'" #запрос на вывод ошибки

        message_result = UnicornMetadata.unicorn_crsr.execute(message_qr).fetchone()

        if message_result != None:
            return message_result[0]
        else: print("Something wrong with system errors. Please, contact your administrator")

    #Метаданные источника
    @staticmethod
    def source_meta(source_type_id, object_type):

        meta_qr = "SELECT OBJECT_NM FROM unicorn_metadata.SOURCE_TYPE_METADATA WHERE SOURCE_TYPE_ID='"+str(source_type_id)+"' AND OBJECT_TYPE='"+object_type+"'" #выводит имя объекта метаданных в зависимости от СУБД
        meta_result = UnicornMetadata.unicorn_crsr.execute(meta_qr).fetchone()

        if meta_result != None:
            meta_result = {"binary_result":1, "value": meta_result[0]}
        else: meta_result = {"binary_result":0, "value":UnicornMetadata.error_message("UnicornMetadataError")}

        return meta_result

    #TDS Version
    @staticmethod
    def tds_version():

        error_metadata = "UnicornMetadataError"
        tds_version = UnicornMetadata.unicorn_crsr.execute( "SELECT TDS_VERSION FROM unicorn_metadata.TDS_VERSION" ).fetchone()
        if tds_version == None:
            result = {"binary_result": 0, "value": UnicornMetadata.error_message(error_metadata)}
        else: result = {"binary_result": 1, "value":tds_version[0]}

        return result

    #Определение SOURCE_TYPE_ID и DRIVER_NAME по SOURCE_TYPE_NM
    @staticmethod
    def src_type_driver(source_type_nm):

        error_metadata = "UnicornMetadataError"

        source_type_driver = UnicornMetadata.unicorn_crsr.execute("SELECT SOURCE_TYPE_ID, DRIVER_NAME FROM unicorn_metadata.SOURCE_TYPE WHERE SOURCE_TYPE_NAME='" + source_type_nm + "'" ).fetchone()
        if source_type_driver == None:
            result = {"binary_result": 0, "value": UnicornMetadata.error_message(error_metadata)}
        else: result = {"binary_result": 1, "source_type_id":source_type_driver[0], "driver_name":source_type_driver[1]}

        return result

    #проверка наименования источника на дубль
    @staticmethod
    def src_nm_check(source_name):

        error_source_name = "SourceNameAlreadyExists"

        check_cnct_nm = UnicornMetadata.unicorn_crsr.execute("SELECT COUNT(*) FROM unicorn_metadata.SOURCES WHERE SOURCE_NAME='" + source_name + "' AND DISABLE_FLG='0'" ).fetchone()
        if check_cnct_nm[0] > 0:
            result = {"binary_result": 0, "text_result": UnicornMetadata.error_message( error_source_name )}
        else: result = {"binary_result": 1, "text_result": "OK"}

        return result

    #проверка на уже существующий источник (сервер, база)
    @staticmethod
    def src_server_db_check(server_name, database):

        error_source = "SourceAlreadyExists"

        src_cnt = UnicornMetadata.unicorn_crsr.execute("SELECT COUNT(*) FROM unicorn_metadata.SOURCES WHERE SERVER_NAME='" + server_name + "' AND DATABASE_NAME='" + database + "' AND DISABLE_FLG='0'" ).fetchone()
        if src_cnt[0] > 0:
            result = {"binary_result": 0, "text_result": UnicornMetadata.error_message(error_source)}
        else: result = {"binary_result": 1, "text_result": "OK"}

        return result


