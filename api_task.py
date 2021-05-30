import mysql.connector as connection
from flask import Flask, render_template, request, jsonify
import csv
import pandas as pd

app = Flask(__name__)


@app.route('/sql_create_table', methods=['POST'])  # 1
def create_table():
    if (request.method == 'POST'):
        host = request.json['host']
        user = request.json['user']
        passwd = request.json['passwd']
        db_name = request.json['db_name']
        table_name = request.json['table_name']
        col1 = request.json['col1']
        col2 = request.json['col2']
        col3 = request.json['col3']
        col4 = request.json['col4']

        try:
            mydb = connection.connect(host=host, user=user, passwd=passwd, use_pure=True)
            query_db = f"""create database IF NOT EXISTS {db_name};"""
            cursor = mydb.cursor()
            cursor.execute(query_db)
            result = "Database {} ".format(db_name)
            # query_table = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ({col1} INT,{col2} VARCHAR(20),{col3} varchar(20),{col4} INT);"""
            mydb1 = connection.connect(host=host, database=db_name, user=user, passwd=passwd, use_pure=True)
            query_table = f"""CREATE TABLE IF NOT EXISTS {table_name} ({col1},{col2},{col3},{col4});"""
            # print(query_table)
            cursor1 = mydb1.cursor()
            cursor1.execute(query_table)
            result1 = "And Table {} is created".format(table_name)
            final = result + result1

            return jsonify(final)

        except Exception as e:
            mydb.close()
            print(str(e))
            return jsonify(str(e))


@app.route('/insert_data_in_sql_table', methods=['POST'])  # 2
def insert_data_table():
    if (request.method == 'POST'):
        host = request.json['host']
        user = request.json['user']
        passwd = request.json['passwd']
        db_name = request.json['db_name']
        table_name = request.json['table_name']
        val1 = request.json['val1']
        val2 = request.json['val2']
        val3 = request.json['val3']
        val4 = request.json['val4']

        try:
            mydb1 = connection.connect(host=host, database=db_name, user=user, passwd=passwd, use_pure=True)
            # query_table = f"""INSERT INTO {table_name} ({col1},{col2},{col3},{col4}) VALUES ({val1},{val2},{val3},{val4});"""
            query_table = 'INSERT INTO {0} VALUES ({1},{2},{3},{4});'.format(table_name, val1, val2, val3, val4)
            cursor1 = mydb1.cursor()
            cursor1.execute(query_table)
            mydb1.commit()
            result1 = "Values succesfully inserted into database {} where table name is {}".format(db_name, table_name)

            return jsonify(result1)

        except Exception as e:
            mydb1.close()
            print(str(e))
            return jsonify(str(e))


@app.route('/update_data_in_sql_table', methods=['POST'])  # 3
def update_data_in_sql_table():
    if (request.method == 'POST'):
        host = request.json['host']
        user = request.json['user']
        passwd = request.json['passwd']
        db_name = request.json['db_name']
        table_name = request.json['table_name']
        col_to_update = request.json['col_to_update']
        col_value_to_update = request.json['col_value_to_update']
        new_col_value = request.json['new_col_value']
        try:
            mydb2 = connection.connect(host=host, database=db_name, user=user, passwd=passwd, use_pure=True)
            query_update = "UPDATE {0} SET {1} = {2} WHERE {3} = {4};".format(table_name, col_to_update, new_col_value,
                                                                              col_to_update, col_value_to_update)
            cursor2 = mydb2.cursor()
            cursor2.execute(query_update)
            mydb2.commit()
            result2 = "Values succesfully updated {} to {} in {}".format(col_value_to_update, new_col_value,
                                                                         col_to_update)

            return jsonify(result2)

        except Exception as e:
            mydb2.close()
            print(str(e))
            return jsonify(str(e))


@app.route('/insert_bulk_data_into_sql', methods=['POST'])  # 4
def insert_bulk_data_into_sql():
    if (request.method == 'POST'):
        host = request.json['host']
        user = request.json['user']
        passwd = request.json['passwd']
        db_name = request.json['db_name']
        table_name = request.json['table_name']
        file_location = request.json['file_location']

        try:
            mydb3 = connection.connect(host=host, user=user, passwd=passwd, use_pure=True)
            with open(file_location, "r") as file1:
                data_csv = csv.reader(file1, delimiter=",")
                col_names = next(data_csv)
                row = next(data_csv)
                #print(row)
                col_type = []
                for j in row:
                    if type(j) == int:
                        # print(j)
                        col_type.append('int')
                    if type(j) == str:
                        # print(j)
                        col_type.append('VARCHAR(20)')

                col_names_final = []
                print(col_names)
                for i in range(len(col_names)):
                    # print(i)
                    col_names_final.append(col_names[i].replace(" ", ""))

                col_names_query = []
                for i in range(len(col_names_final)):
                    col_names_query.append(col_names_final[i] + " " + col_type[i])

                query_db = f"""create database IF NOT EXISTS {db_name};"""
                cursor = mydb3.cursor()
                cursor.execute(query_db)

                query_table = "CREATE TABLE IF NOT EXISTS {}.{} ({});".format(db_name, table_name, ",".join(
                    [value for value in col_names_query]))
                # print(query_table)
                cursor1 = mydb3.cursor()  # create a cursor to execute queries
                cursor1.execute(query_table)

                with open(file_location, "r") as file2:
                    data_csv1 = csv.reader(file2, delimiter=",")
                    next(data_csv1)

                    for i in data_csv1:
                        #print(i)
                        row = str(i).replace('[', '').replace(']', '')
                        query = f"""INSERT INTO {db_name}.{table_name} values({row});"""  # values = ", ".join([value for value in i])
                        # print(values)
                        cursor4 = mydb3.cursor()
                        cursor4.execute(query)
                    mydb3.commit()

                result = f""""Database {db_name} and table {table_name} is created and data inseted into it successfully"""
                return jsonify(result)
        except Exception as e:
            mydb3.close()
            print(str(e))
            return jsonify(str(e))


@app.route('/delete_from_sql_table', methods=['POST'])  # 5
def delete_from_sql_table():
    if (request.method == 'POST'):
        host = request.json['host']
        user = request.json['user']
        passwd = request.json['passwd']
        db_name = request.json['db_name']
        table_name = request.json['table_name']
        item_col_name_to_del_from_table = request.json['item_col_name_to_del_from_table']
        item_to_del_from_table = request.json['item_to_del_from_table']

        try:
            mydb = connection.connect(host=host, database=db_name, user=user, passwd=passwd, use_pure=True)
            query_del = f"""DELETE FROM {table_name} WHERE {item_col_name_to_del_from_table}={item_to_del_from_table};"""
            cursor = mydb.cursor()  # create a cursor to execute queries
            cursor.execute(query_del)
            mydb.commit()
            result = f"""{item_to_del_from_table} is deleted from {table_name}"""

            return jsonify(result)

        except Exception as e:
            mydb.close()
            print(str(e))
            return jsonify(str(e))


@app.route('/download_data_from_sql', methods=['POST'])  # 6
def download_data_from_sql():
    if (request.method == 'POST'):
        host = request.json['host']
        user = request.json['user']
        passwd = request.json['passwd']
        db_name = request.json['db_name']
        table_name = request.json['table_name']
        try:
            mydb = connection.connect(host=host, database=db_name, user=user, passwd=passwd, use_pure=True)
            query_download = f"""select * from {table_name};"""
            data = pd.read_sql(query_download, mydb)
            data.to_csv('export.csv', index=False)
            mydb.close()
            result = f"""Successfully downloaded"""

            return jsonify(result)


        except Exception as e:
            mydb.close()
            print(str(e))
            return jsonify(str(e))


if __name__ == '__main__':
    app.run()
