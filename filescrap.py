
import os
from pathlib import Path
import mysql.connector

#create TABLE employees(id int PRIMARY key AUTO_INCREMENT,empname varchar(100),empemail varchar(100),empcontact varchar(12));

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='demo_db',
                                         user='root',
                                         password='')
                        
    # Get the list of all files and directories
    path = "E://employees"
    dir_list = os.listdir(path)
    
    #print("Files and directories in '", path, "' :")
    # prints all files
    #print(dir_list)
    recordlist=[]
    for files in dir_list:
        fetchedrecord=Path(path+"//"+files).read_text().split()
        templist=[]
        for wrd in fetchedrecord:
            if wrd.isdigit():
                templist.append(int(wrd),)
            else:
                templist.append(wrd,)
        recordlist.append(tuple(templist))

    print(recordlist)

    mySql_insert_query = """INSERT INTO employees (id, empname, empemail, empcontact) 
                           VALUES (%s, %s, %s, %s) """


    cursor = connection.cursor()
    cursor.executemany(mySql_insert_query, recordlist)
    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into employees table")

except mysql.connector.Error as error:
    print("Failed to insert record into MySQL table {}".format(error))

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

