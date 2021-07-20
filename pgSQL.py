# Skrypt pozwala na nawiązanie połączenia z bazą danych PostgreSQL i zbudowanej na niej tabeli. Następnie dane z pliku csv są przekopiowywane do nowo utworzonej 
# tabeli.

# Import paczek
import psycopg2
import sys
import os
from psycopg2 import OperationalError

# Utworzenie klasy NewTable
class NewTable:
    
      def __init__(self):
          
          self.conn=None
          
        # Nawiązanie połączenia z bazą danych
          try:
              self.conn=psycopg2.connect("dbname='postgres' user='postgres' password='1234' host='localhost' port='5433'")
              self.cur=self.conn.cursor()
              print("Connection successfully..................")
                           
         # Utworzenie wyjątku w przypadku błędów       
          except OperationalError as err: 
              show_psycopg2_exception(err)   
              self.conn = None 
    
      def create_table(self):
          try:
            # Usunięcie tabeli w przypadku, gdy istnieje już taka
              self.cur.execute("DROP TABLE IF EXISTS rating_beauty;")
            # Utworzenie nowej tabeli rating_beauty
              self.cur.execute("CREATE TABLE rating_beauty (UserId CHAR(35) NOT NULL, ProductId TEXT NOT NULL, Rating REAL, Timestamp REAL )")
              self.conn.commit()
              print("Table is created successfully...............")
                          
          except OperationalError as err:
             show_psycopg2_exception(err)
             self.conn = None
          
      def copy_data(self, filepath):
          try:
            # Otworzenie pliku z podanej ścieżki i odkodowanie go    
              with open(filepath, 'r', encoding="utf8") as f:
                  # Pominięcie nagłówków 
                  next(f) 
                  # Przekopiowanie danych z pliku csv do tabeli rating_beauty
                  self.cur.copy_expert("copy rating_beauty from stdin (format csv) ", f)
              self.conn.commit()
              print("Data inserted to database succesfully.............")
          except (Exception, psycopg2.DatabaseError) as err:
            show_psycopg2_exception(err)
            

          
# Funkcja do zwracania błędów w przypadku połączenia z bazą danych, kod skopiowany z: 
# https://medium.com/analytics-vidhya/part-4-pandas-dataframe-to-postgresql-using-python-8ffdb0323c09

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


