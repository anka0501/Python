# First step of project. I build my database in a PostgreSQL tool. 
# This part of project consist of: establish connection from Python to a 
# PostgreSQL database thanks of psycopg2 libraries, create new table and 
# import csv file.

# Import libraries
import psycopg2
import sys
import os
from psycopg2 import OperationalError


class NewTable:
    
      def __init__(self):
          
          self.conn=None
          
          try:
              self.conn=psycopg2.connect("dbname='postgres' user='postgres' password='1234' host='localhost' port='5433'")
              self.cur=self.conn.cursor()
              print("Connection successfully..................")
                           
          except OperationalError as err:
              # passing exception to function  
              show_psycopg2_exception(err)   
              # set the connection to 'None' in case of error
              self.conn = None 
    
      def create_table(self):
          try:
              self.cur.execute("DROP TABLE IF EXISTS rating_beauty;")
              self.cur.execute("CREATE TABLE rating_beauty (UserId CHAR(35) NOT NULL, ProductId TEXT NOT NULL, Rating REAL, Timestamp REAL )")
              self.conn.commit()
              print("Table is created successfully...............")
                          
          except OperationalError as err:
              # pass exception to function
             show_psycopg2_exception(err)
             # set the connection to 'None' in case of error
             self.conn = None
          
      def copy_data(self, filepath):
          try:
              with open(filepath, 'r', encoding="utf8") as f:
                  # skip the header row
                  next(f) 
                  self.cur.copy_expert("copy rating_beauty from stdin (format csv) ", f)
              self.conn.commit()
              print("Data inserted to database succesfully.............")
          except (Exception, psycopg2.DatabaseError) as err:
            # pass exception to function
            show_psycopg2_exception(err)
            

          
# Define a function that handles and parses psycopg2 exceptions
def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()    
    # get the line number when exception occured
    line_n = traceback.tb_lineno    
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)    
    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")
            
            
new_table=NewTable()

new_table.create_table()

new_table.copy_data(r'..\Maszyna Boltzmana\beauty.csv')


