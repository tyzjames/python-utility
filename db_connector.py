# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 14:24:35 2017
Wrappers for connecting to database
@author: james.toh
"""

class mysql_wrapper:
    
    def __init__(self, database, host, user, password):
        
        self.db = database
        self.host = host
        self.user = user
        self.password = password
        try:
            self.cur = __import__('pymysql.cursors')
            self.pd = __import__('pandas')
        except BaseException as e:
            raise(e)
            
        self.connection = None
    
    def connect(self, local_infile = False):
        self.close()
        
        try:
            self.connection = self.cur.connect(host=self.host,
                                     user=self.user,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=self.cur.cursors.DictCursor,
                                     local_infile = local_infile)
        except BaseException as e:
            raise(e)
            
    def getData(self, sql):
        result = {}
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
        except BaseException as e:
            raise (e)

        if len(result) == 0:
            return None
        return self.pd.DataFrame(result)
    
    def execute(self, sql):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
        except BaseException as e:
            raise(e)
        return True
        
    def close(self):
        try:
            self.connection.close()
        except:
            pass

class gp_wrapper:
    
    def __init__(self, database, host, port, user, password):
        
        try:
            self.psycopg2 = __import__('psycopg2')
            self.pd = __import__('pandas')
            self.psql = __import__('pandas.io.sql')
        except BaseException as e:
            print(e)
            raise BaseException("Error importing packages")
            
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.conn_str =  """dbname='{database}' user='{user}' host='{host}' port='{port}' password='{password}'""".format(                       
                            database=self.database,
                            host=self.host,
                            port=self.port,
                            user=self.user,
                            password=self.password
                    )
        
    def connect(self):
        self.conn = self.psycopg2.connect(self.conn_str)
        self.cur = self.conn.cursor()
    
    def close(self):
        self.cur.close()
        self.conn.close()
        
    def commit(self):
        self.conn.commit()
        
    def rollback(self):
        self.conn.rollback()
        
    def get_data_chunk(self, sql_str, chunk_size = 5000):
        df_main = None
        is_conn_open = True; ## Close the connection later if it is not already opened
        if self.conn.closed == 1:
            self.connect()
            is_conn_open= False;
        
        try:
            # x = self.psql.read_sql(sql_str, self.conn)
            self.cur.execute(sql_str)
            df_main = self.pd.DataFrame(self.cur.fetchmany(chunk_size))
            df_main.columns = [i[0] for i in self.cur.description]
            counter = 1
            while True:
                df_temp = self.pd.DataFrame(self.cur.fetchmany(chunk_size))
                if len(df_temp) == 0:
                    break;
                else:
                    counter += 1
                    print("Getting chunk: %s" % str(counter))
                    df_main = df_main.append(df_temp)
                    del df_temp
                
        except BaseException as e:
            raise(e)
        finally:
            if not is_conn_open:
                self.close()
        return df_main
        
    def getData(self, sql_str):
        x = None
        is_conn_open = True; ## Close the connection later if it is not already opened
        if self.conn.closed == 1:
            self.connect()
            is_conn_open= False;
        try:
            # x = self.psql.read_sql(sql_str, self.conn)
            self.cur.execute(sql_str)
            x = self.cur.fetchall()
            x = self.pd.DataFrame(x)
            x.columns = [i[0] for i in self.cur.description]
        except BaseException as e:
            raise(e)
        finally:
            if not is_conn_open:
                self.close()
        return x

    def execute(self,sql_str):
        is_conn_open = True; ## Close the connection later if it is not already opened
        if self.conn.closed == 1:
            self.connect()
            is_conn_open= False;
        try:
            self.cur.execute(sql_str)
        except BaseException as e:
            print(e)
            raise BaseException(e)
        finally:
            if not is_conn_open:
                self.close()