import mysql.connector as connection
from flask import Flask, request, jsonify
import csv
import pandas as pd
from flask.views import MethodView
import datetime
from functools import wraps
import os, sys,logging

app = Flask(__name__)

cwd = os.getcwd()
folder = 'Logs'
newPath = os.path.join(cwd, folder)

try:
    """try to create a folder"""
    os.mkdir(newPath)

except Exception as e:
    logging.basicConfig(filename='{}/Log_file.log'.format(newPath), level=logging.DEBUG)


def moniter(function):

    try:
        @wraps(function)
        def wrapper(*args, **kwargs):
            s = datetime.datetime.now()
            print("="*60)
            ip_address = "{}".format(request.remote_addr)
            user_agent = "{}".format(request.user_agent)
            called_fuction_name = "{}".format(function.__name__)
            _ = function(*args, **kwargs)
            called_arguments = dict(kwargs)
            called_function_arguments = '{}'.format(called_arguments)
            e = datetime.datetime.now()
            exe_time = "{}".format((e - s))
            called_function_size = "{} Bytes".format(sys.getsizeof(function))
            end_time = "{}".format(e)

            print("Start time : ", s)
            print("IP_address : ",ip_address)
            print("user Agent : ",user_agent)
            print("Called function Name : ",called_fuction_name)
            print("CalledFunction Arguments : ",called_function_arguments)
            print("Memory : ",called_function_size)
            print("Called function Execution Time : ", exe_time)
            print("End Time : ",end_time)
            print("=" * 60)

            message = """
                Start time : {}
                IP_adsress : {}
                User Agent : {}
                Called Fuction Name : {}
                Called Function Arguments : {}
                Memory : {}
                Execution Time : {}
                End Time : {}
                """.format(s,ip_address,user_agent,called_fuction_name,called_function_arguments,called_function_size,exe_time,e)

            logging.debug(message)

            return _

        return wrapper

    except Exception as e:
        logging.error("ERROR : {}".format(str(e)))

class Database(MethodView):


    @moniter
    def establish_conn(self,host, user, passwd):
        """
        This function establishes connectivity to sql database
        :param host:
        :param user:
        :param passwd:
        :return:
        """
        try:
            return connection.connect(host=host, user=user, passwd=passwd, use_pure=True)
        except Exception as e:
            logging.error(f""" ERROR IN : establish_conn, ERROR : {str(e)}""")
            return f""" ERROR IN : establish_conn, ERROR : {str(e)}"""

    @moniter
    def create_msql_db(self, mydb, db_name):
        """
        This function is for creating database in mysql
        :param mydb:
        :param db_name:
        :return:
        """
        try:
            cursor = mydb.cursor()
            query_db = f"""create database IF NOT EXISTS {db_name};"""
            cursor.execute(query_db)
            logging.debug(f""" Database : {db_name} is Created Successfully\n""")
            return f""" Database : {db_name} is Created Successfully\n"""

        except Exception as e:
            logging.error(f""" ERROR IN : create_msql_db : {str(e)}\n""")
            return f"""ERROR IN : create_msql_db : {str(e)}\n"""

    @moniter
    def create_mysql_table(self, mydb, db_name, table_name, table_query):
        """
        This function is for creating table in particular database.
        :param mydb:
        :param db_name:
        :param table_name:
        :param table_query:
        :return:
        """

        try:
            cursor = mydb.cursor()
            query_table = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ({table_query});"""
            cursor.execute(query_table)
            logging.debug(f""" Table : {table_name} is Created Successfully\n""")
            return f""" Table : {table_name} is Created Successfully\n"""

        except Exception as e:
            logging.error(f""" ERROR IN : create_mysql_table : {str(e)}\n""")
            return f"""ERROR IN : create_mysql_table : {str(e)}\n"""

    @moniter
    def reformat_table_structure(self, table_elements):
        """
        This function is to reformat table column names and type.
        :param table_elements:
        :return:
        """
        try:
            table_query = ''
            if type(table_elements) == dict:
                for i in table_elements:
                    table_query += i + ' ' + str(table_elements[i]) + ', '
                # print(table_query,"\n")
                table_query = table_query[:-2]
                print("table_query", table_query)
                logging.debug(" table_elements reformatted successfully")
                return table_query
            else:
                logging.debug(f""" There is problem with data type of table_elements must be in dictionary format\n""")
                return f"""There is problem with data type of table_elements must be in dictionary format\n"""

        except Exception as e:
            logging.error(f""" ERROR IN : reformat_table_structure : {str(e)}""")
            return f"""ERROR IN : reformat_table_structure : {str(e)}"""

    @moniter
    def reformat_table_data_for_insertion(self, data_to_insert):
        """
        This function is to reformat table data for insertion.
        :param data_to_insert:
        :return:
        """
        try:
            col_names = ''
            col_data = ''
            if type(data_to_insert) == dict:
                for i in data_to_insert:
                    col_names += i + ', '
                    if type(data_to_insert[i]) == str:
                        col_data += '"' + data_to_insert[i] + '"' + ', '
                    else:
                        col_data += str(data_to_insert[i]) + ', '

                col_names = col_names[:-2]
                col_data = col_data[:-2]
                logging.debug(" Data successfully reformatted for insertion\n")

                return col_names, col_data
            else:
                logging.debug(f""" There is problem with data type of data_to_insert must be in dictionary format\n""")
                return f"""There is problem with data type of data_to_insert must be in dictionary format\n"""

        except Exception as e:
            logging.error(f"""ERROR IN : reformat_table_data_for_insertion ERROR : {str(e)}""")
            return f"""ERROR IN : reformat_table_data_for_insertion ERROR : {str(e)}"""

    @moniter
    def reformat_table_data_for_update(self, data_to_update):
        """
        This function is to reformat table data for update.
        :param data_to_update:
        :return:
        """
        try:
            update_col = ''
            if type(data_to_update) == dict:
                for i in data_to_update:
                    update_col += i + ' = '
                    if type(data_to_update[i]) == str:
                        update_col += '"' + data_to_update[i] + '"' + ', '
                    else:
                        update_col += str(data_to_update[i]) + ', '
                update_col = update_col[:-2]
                logging.debug(" Data successfully reformatted for update\n")
                return update_col
            else:
                logging.debug(f"""There is problem with data type of data_to_update must be in dictionary format\n""")
                return f"""There is problem with data type of data_to_update must be in dictionary format\n"""

        except Exception as e:
            logging.error(f"""ERROR IN : reformat_table_data_for_update ERROR : {str(e)}""")
            return f"""ERROR IN : reformat_table_data_for_update ERROR : {str(e)}"""

    @moniter
    def reformat_selection_criteria(self, selection_criteria):
        """
        This function is to reformat selection criteria for update,delete and download data.
        :param selection_criteria:
        :return:
        """
        try:
            selection_col = 'where '
            if type(selection_criteria) == dict:
                for i in selection_criteria:
                    selection_col += i + ' = '
                    if type(selection_criteria[i]) == str:
                        selection_col += '"' + selection_criteria[i] + '"' + ' and '
                    else:
                        selection_col += str(selection_criteria[i]) + ' and '
                selection_col = selection_col[:-5]
                logging.debug(" Data successfully reformatted selection_criteria")
                return selection_col

            else:
                logging.debug(f"""There is problem with data type of selection_criteria must be in dictionary format\n""")
                return f"""There is problem with data type of selection_criteria must be in dictionary format\n"""

        except Exception as e:
            logging.error(f"""ERROR IN : reformat_selection_criteria : {str(e)}""")
            return f"""ERROR IN : reformat_selection_criteria : {str(e)}"""

    @moniter
    def reformat_file_for_bulk_data_insertion(self, file_location):
        """
        In this function we need to get col_names for creating table for particular
        csv file.
        And we are also concatenating particular col_names with its datatype so that we can
        use that is query.
        :param file_location:
        :return:
        """
        try:
            with open(file_location, "r") as file1:
                data_csv = csv.reader(file1, delimiter=",")
                col_names = next(data_csv)
                row = next(data_csv)
                col_type = []
                for j in row:
                    if type(j) == int:
                        col_type.append('int')
                    if type(j) == str:
                        col_type.append('VARCHAR(20)')

                col_names_final = []
                for i in range(len(col_names)):
                    col_names_final.append(col_names[i].replace(" ", ""))

                col_names_query = []
                for i in range(len(col_names_final)):
                    col_names_query.append(col_names_final[i] + " " + col_type[i])

                col_names_query = ",".join([value for value in col_names_query])
                logging.debug(" reformat_file_for_bulk_data_insertion is done")
                return col_names_query

        except Exception as e:
            logging.error(f"""ERROR IN : reformat_file_for_bulk_data_insertion, ERROR : {str(e)}""")
            return f"""ERROR IN : reformat_file_for_bulk_data_insertion, ERROR : {str(e)}"""

    @moniter
    def insert_data_in_mysql(self, mydb, db_name, table_name, col_names, col_data):
        """
        This function is to insert data into mysql.
        :param mydb:
        :param db_name:
        :param table_name:
        :param col_names:
        :param col_data:
        :return:
        """
        try:
            cursor = mydb.cursor()
            query_insert = "INSERT INTO {0}.{1} VALUES ({2});".format(db_name, table_name, col_data)
            cursor.execute(query_insert)
            mydb.commit()
            logging.debug(f""" Data Successfully Inserted inside Database : {db_name} and Table Name : {table_name}""")
            return f"""Data Successfully Inserted inside Database : {db_name} and Table Name : {table_name}"""

        except Exception as e:
            logging.error(f""" ERROR IN : insert_data_in_mysql, ERROR  : {str(e)}""")
            return f"""ERROR IN : insert_data_in_mysql, ERROR  : {str(e)}"""

    @moniter
    def update_data_in_sql(self, mydb, db_name, table_name, update_col, selection_col):
        """
        This fuction is to update data into mysql
        :param mydb:
        :param db_name:
        :param table_name:
        :param update_col:
        :param selection_col:
        :return:
        """
        try:
            cursor = mydb.cursor()
            query_update = f"""UPDATE {db_name}.{table_name} set {update_col} {selection_col};"""
            print("query_update", query_update)
            cursor.execute(query_update)
            mydb.commit()
            logging.debug(f""" Data Successfully updated inside\nDatabase : {db_name}\n Table Name : {table_name}\n""")
            return f"""Data Successfully updated inside\nDatabase : {db_name}\n Table Name : {table_name}\n"""

        except Exception as e:
            logging.error(f"""ERROR IN : upade_data_in_sql(), ERROR : {str(e)}""")
            return f"""ERROR IN : upade_data_in_sql(), ERROR : {str(e)}"""

    @moniter
    def inserting_bulk_data_in_sql(self, mydb, db_name, table_name, file_location):
        """
        This function is for inserting_bulk_data_in_sql
        :param mydb:
        :param db_name:
        :param table_name:
        :param file_location:
        :return:
        """
        try:
            with open(file_location, "r") as file:
                data_csv = csv.reader(file, delimiter=",")
                next(data_csv)
                count_suc = 0
                count_fail = 0
                for i in data_csv:
                    # print(i)
                    row = str(i).replace('[', '').replace(']', '')
                    query = f"""INSERT INTO {db_name}.{table_name} values({row});"""  # values = ", ".join([value for value in i])
                    # print(values)
                    cursor = mydb.cursor()
                    cursor.execute(query)
                    count_suc += 1
                mydb.commit()
                logging.debug(f""" Total {count_suc} records inserted successfully""")
                return f"""Total {count_suc} records inserted successfully"""

        except Exception as e:
            logging.error(f"""ERROR IN : inserting_bulk_data_in_sql(), ERROR : {str(e)}""")
            return f"""ERROR IN : inserting_bulk_data_in_sql(), ERROR : {str(e)}"""

    @moniter
    def delete_data_from_table(self, mydb, db_name, table_name, selection_col):
        """
        This function os for delete_data_from_table
        :param mydb:
        :param db_name:
        :param table_name:
        :param selection_col:
        :return:
        """
        try:
            cursor = mydb.cursor()
            query_del = f"""delete from {db_name}.{table_name} {selection_col};"""
            cursor.execute(query_del)
            mydb.commit()
            logging.debug(f""" Data {selection_col} is successfully deleted from database {db_name} and table {table_name}""")
            return f"""Data {selection_col} is successfully deleted from database {db_name} and table {table_name}"""
        except Exception as e:
            logging.error(f""" ERROR IN : delete_data_from_table(), ERROR : {str(e)}""")
            return f"""ERROR IN : delete_data_from_table(), ERROR : {str(e)}"""
    @moniter
    def download_data_from_table(self, mydb, db_name, table_name, selection_col, file_name_for_download,file_download_location):
        """
        This function is for download_data_from_table
        :param mydb:
        :param db_name:
        :param table_name:
        :param selection_col:
        :param file_name_for_download:
        :param file_download_location:
        :return:
        """
        try:
            query_download = f"""select * from {db_name}.{table_name} {selection_col};"""
            data = pd.read_sql(query_download, mydb)

            data.to_csv(f"""{file_download_location}\\{file_name_for_download}.csv""", index=False)
            logging.debug(f""" All records successfully downloaded at {file_download_location}\\{file_name_for_download}.csv""")

            return f"""All records successfully downloaded at {file_download_location}\\{file_name_for_download}.csv"""

        except Exception as e:
            logging.error(f"""ERROR IN : download_data_from_table(), ERROR : {str(e)}""")
            return f"""ERROR IN : download_data_from_table(), ERROR : {str(e)}"""

    @moniter
    def post(self):
        """
        For this you need to use POST method on postman to submit request.
        """

        try:
            host = request.json['host']
            user = request.json['user']
            passwd = request.json['passwd']
            operation_to_perform = request.json['operation_to_perform']
            db_name = request.json['db_name']
            table_name = request.json['table_name']


            try:
                if operation_to_perform == 'create_table':
                    table_elements = request.json['table_elements']
                    mydb = self.establish_conn(host=host, user=user, passwd=passwd)
                    db_crate = self.create_msql_db(mydb=mydb, db_name=db_name)
                    table_query = self.reformat_table_structure(table_elements=table_elements)
                    table_create = self.create_mysql_table(mydb=mydb, db_name=db_name, table_name=table_name, table_query=table_query)
                    logging.debug(db_crate)
                    logging.debug(table_create)
                    return jsonify(db_crate, table_create)

            except Exception as e:
                res = f"""ERROR IN : def post() method where condition is {operation_to_perform} and ERROR = {str(e)}"""
                logging.error(res)
                return jsonify(res)

            try:
                if operation_to_perform == 'insert_data_table':
                    data_to_insert = request.json['data_to_insert']
                    mydb = self.establish_conn(host=host, user=user, passwd=passwd)
                    col_names, col_data = self.reformat_table_data_for_insertion(data_to_insert=data_to_insert)
                    result = self.insert_data_in_mysql(mydb=mydb, db_name=db_name, table_name=table_name, col_names=col_names, col_data=col_data)
                    logging.debug(result)
                    return jsonify(result)

            except Exception as e:
                res = f""" ERROR IN : def post() method where condition is {operation_to_perform} and ERROR = {str(e)}"""
                logging.error(res)
                return jsonify(res)

            try:
                if operation_to_perform == 'update_data_into_table':
                    data_to_update = request.json['data_to_update']
                    selection_criteria = request.json['selection_criteria']
                    mydb = self.establish_conn(host=host, user=user, passwd=passwd)
                    update_col = self.reformat_table_data_for_update(data_to_update=data_to_update)
                    selection_col = self.reformat_selection_criteria(selection_criteria=selection_criteria)
                    result = self.update_data_in_sql(mydb=mydb, db_name=db_name, table_name=table_name, update_col=update_col, selection_col=selection_col)
                    logging.debug(result)
                    return jsonify(result)

            except Exception as e:
                res = f""" ERROR IN : def post() method where condition is {operation_to_perform} and ERROR = {str(e)}"""
                logging.error(res)
                return jsonify(res)

            try:
                if operation_to_perform == 'bulk_insertion_into_sql':
                    file_location = request.json['file_location']
                    mydb = self.establish_conn(host=host, user=user, passwd=passwd)
                    col_names_query = self.reformat_file_for_bulk_data_insertion(file_location=file_location)
                    db_create = self.create_msql_db(mydb=mydb, db_name=db_name)
                    table_create = self.create_mysql_table(mydb=mydb, db_name=db_name, table_name=table_name, table_query=col_names_query)
                    result = self.inserting_bulk_data_in_sql(mydb=mydb, db_name=db_name, table_name=table_name, file_location=file_location)
                    logging.debug(db_create)
                    logging.debug(table_create)
                    logging.debug(result)
                    return jsonify(db_create, table_create, result)

            except Exception as e:
                res = f""" ERROR IN : def post() method where condition is {operation_to_perform} and ERROR = {str(e)}"""
                logging.error(res)
                return jsonify(f"""ERROR IN : def post() method where condition is {operation_to_perform} and ERROR = {str(e)}""")

            try:
                if operation_to_perform == 'delete_data_from_table':
                    selection_criteria = request.json['selection_criteria']
                    selection_col = self.reformat_selection_criteria(selection_criteria=selection_criteria)
                    mydb = self.establish_conn(host=host, user=user, passwd=passwd)
                    result = self.delete_data_from_table(mydb=mydb, db_name=db_name, table_name=table_name, selection_col=selection_col)
                    logging.debug(result)
                    return jsonify(result)

            except Exception as e:
                res = f""" ERROR IN : def post() method where condition is {operation_to_perform} and ERROR = {str(e)}"""
                logging.error(res)
                return jsonify(res)

            try:
                if operation_to_perform == 'download_data_from_table':
                    selection_criteria = request.json['selection_criteria']
                    file_name_for_download = request.json['file_name_for_download']
                    file_download_location = request.json['file_download_location']

                    if selection_criteria == '' or '*':
                        selection_col = ''
                    else:
                        selection_col = self.reformat_selection_criteria(selection_criteria=selection_criteria)
                    mydb = self.establish_conn(host=host, user=user, passwd=passwd)
                    result = self.download_data_from_table(mydb=mydb, db_name=db_name, table_name=table_name, selection_col=selection_col,
                                                           file_name_for_download=file_name_for_download, file_download_location=file_download_location)
                    logging.debug(result)
                    return jsonify(result)

                else:
                    res = f""" please select valid operation_to_perform"""
                    logging.error(res)
                    return jsonify(f"""please select valid operation_to_perform""")

            except Exception as e:
                res = f""" ERROR IN : def post() method where condition is {operation_to_perform} and ERROR = {str(e)}"""
                logging.error(res)
                return jsonify(res)

        except Exception as e:
            logging.error(f""" ERROR IN : def post() method, ERROR : {str(e)}""")
            return jsonify(f"""ERROR IN : def post() method, ERROR : {str(e)}""")


view = Database.as_view("database")
app.add_url_rule("/sql_operations_api", methods=['POST'], view_func=view)

if __name__ == '__main__':
    app.run()

