#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd 
import csv,io,sys ,os
import cx_Oracle
import traceback
from tqdm import tqdm
import time

# In[ ]:


class Oracle:
    
    def Leer_Script_Archivo(paPath,paConn):
        try:
            if os.path.exists(paPath):
                with open(paPath, "r") as file:
                    sql_script = file.read()           
                conn = cx_Oracle.connect(paConn)     
                cursor= conn.cursor() 
                cursor.execute(sql_script)
                conn.commit()
                sw=True
        except Exception as e:
            print("Error de lectura", e)
            sw=False
        return sw
    def ExecuteOperation(conn_id,paSQL):
        try:
            cn=cx_Oracle.connect(conn_id)
            cr = cn.cursor()
            cr.execute(paSQL)
            cn.commit()
            print("Operacion Realizada....")
        except Exception as e:
            print("Error: ",e)
    def ExecuteStoredProcedure(conn_id,sp_name,sp_params):
        cn=cx_Oracle.connect(conn_id)
        cr=cn.cursor()
        output = cr.var(str)
        cr.callproc(sp_name,sp_params)
        return output.getvalue()
    
    def ExecuteStoredProcedure(conn_id,sp_name):
        cn=cx_Oracle.connect(conn_id)
        cr=cn.cursor()
        output = cr.var(str)
        cr.callproc(sp_name)
        return output.getvalue()
    def RetornoData(conn_id,paSQL):
        try:
            cn=cx_Oracle.connect(conn_id) 
            cr = cn.cursor()
            cr.execute(paSQL) 
            rows = cr.fetchall()
            columns = [col[0] for col in cr.description]  # Obtener nombres de columnas
            df = pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print("Error de lectura", e)
        return df
    
    def ExecuteData(conn_id,paSQL):
        try:
            cn=cx_Oracle.connect(conn_id) 
            cr = cn.cursor()
            cr.execute(paSQL) 
            cn.commit()
            sw=True
        except Exception as e:
            print("Error de lectura", e)
            sw=False 
        return sw
    
    def insert_data_dataframe_oracle(df_fin, table, connection_id, partition_size=100000):
        connection = None
        cursor = None
        try:
            connection = cx_Oracle.connect(connection_id)
            cursor = connection.cursor()

            columns = ', '.join(df_fin.columns)
            values_placeholders = ', '.join([':' + str(i + 1) for i in range(len(df_fin.columns))])
            table_name = table

            data = [tuple(row) for row in df_fin.values]
            insert_query = f"INSERT INTO {table_name} VALUES ({values_placeholders})"

            if len(data) > partition_size:
                start_pos = 0
                partition_q = int(df_fin.shape[0] / partition_size)
                print(f'Se divide el dataframe en {partition_q} partes')
                while start_pos < len(data):
                    data_part = data[start_pos:start_pos + partition_size]
                    start_pos += partition_size
                    try:
                        cursor.executemany(insert_query, data_part)
                    except Exception as e:
                        print(f"Error al insertar el bloque desde {start_pos - partition_size} hasta {start_pos}")
                        print("Tipo de error:", type(e).__name__)
                        print("Detalle:", e)
                        for row in data_part:
                            try:
                                cursor.execute(insert_query, row)
                            except Exception as inner_e:
                                print("Error en fila individual:")
                                print("Fila:", row)
                                print("Tipo de error:", type(inner_e).__name__)
                                print("Detalle:", inner_e)
                                # Armamos el INSERT con valores visibles para detectar el problema
                                insert_visible = f"INSERT INTO {table_name} ({columns}) VALUES ("
                                insert_visible += ', '.join([f"'{str(v)}'" if v is not None else "NULL" for v in row])
                                insert_visible += ")"
                                print("QUERY FALLIDA:", insert_visible)

                                for i, value in enumerate(row):
                                    try:
                                        df_fin.iloc[0, i]  # Detectar posibles errores por índice
                                    except Exception as field_e:
                                        print(f"Posible error en campo '{df_fin.columns[i]}':", field_e)
                                raise
            else:
                try:
                    cursor.executemany(insert_query, data)
                except Exception as e:
                    print("Error al insertar el conjunto completo:")
                    print("Tipo de error:", type(e).__name__)
                    print("Detalle:", e)
                    for row in data:
                        try:
                            cursor.execute(insert_query, row)
                        except Exception as inner_e:
                            print("Error en fila individual:")
                            print("Fila:", row)
                            print("Tipo de error:", type(inner_e).__name__)
                            print("Detalle:", inner_e)
                            insert_visible = f"INSERT INTO {table_name} ({columns}) VALUES ("
                            insert_visible += ', '.join([f"'{str(v)}'" if v is not None else "NULL" for v in row])
                            insert_visible += ")"
                            print("QUERY FALLIDA:", insert_visible)
                            
                            for i, value in enumerate(row):
                                try:
                                    df_fin.iloc[0, i]
                                except Exception as field_e:
                                    print(f"Posible error en campo '{df_fin.columns[i]}':", field_e)
                            raise

            connection.commit()
            print(f"Se insertaron {len(data)} filas en la tabla {table_name}.")

        except cx_Oracle.DatabaseError as db_err:
            print("Error de base de datos:", db_err)
            traceback.print_exc()
        except Exception as e:
            print("Error general:", e)
            traceback.print_exc()
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if connection:
                try:
                    connection.close()
                except:
                    pass
                    
    def insert_dataframe_oracle(df_fin, table, connection_id, partition_size=100000):

        connection = cx_Oracle.connect(connection_id)
        
        columns = ', '.join(df_fin.columns)
        values = ', '.join([':' + str(i + 1) for i in range(len(df_fin.columns))])
        table_name = table

        # Convierto la lista de valores en tupla para poder insertarla en la tabla
        data = [tuple(row) for row in df_fin.values]

        # Creo la sentencia de insert en base a las variables 
        insert_query = f"INSERT INTO {table_name} VALUES ({','.join([':' + str(i + 1) for i in range(len(df_fin.columns))])})"

        # Traigo la conexion, creo el cursor y ejecuto el insert.
        cursor = connection.cursor()
        if len(data) > partition_size:
            start_pos = 0
            partition_q = int(df_fin.shape[0]/partition_size)
            print('Se divide el dataframe en {0} partes'.format(partition_q))
            while start_pos < len(data):
                data_part = data[start_pos:start_pos + partition_size]
                start_pos += partition_size
                cursor.executemany(insert_query, data_part)
        else:
            cursor.executemany(insert_query, data)

        # Commit y close.
        connection.commit()
        cursor.close()
        connection.close()

        print(f"Se insertaron {len(data)} filas en la tabla {table_name}.")
        
        
   
    
    def insert_dataframe_oracle_bis(df_fin, table, connection_id, partition_size=100000):
        connection = cx_Oracle.connect(connection_id)
        
        values = ', '.join([':' + str(i + 1) for i in range(len(df_fin.columns))])
        table_name = table

        # Convertir DataFrame a lista de tuplas
        data = [tuple(row) for row in df_fin.values]
        insert_query = f"INSERT INTO {table_name} VALUES ({values})"

        cursor = connection.cursor()

        total_rows = len(data)

        with tqdm(total=total_rows, desc="Insertando registros", unit="fila") as pbar:
            if total_rows > partition_size:
                start_pos = 0
                while start_pos < total_rows:
                    data_part = data[start_pos:start_pos + partition_size]
                    cursor.executemany(insert_query, data_part)
                    pbar.update(len(data_part))  # Actualiza barra por cantidad insertada
                    start_pos += partition_size
            else:
                cursor.executemany(insert_query, data)
                pbar.update(total_rows)

        connection.commit()
        cursor.close()
        connection.close()

        print(f"Se insertaron {len(data)} filas en la tabla {table_name}.")
    
    
def insert_dataframe_oracle_3(df_fin, table, connection_id, partition_size=100000):
    connection = None
    cursor = None
    try:
        connection = cx_Oracle.connect(connection_id)
        cursor = connection.cursor()

        columns = ', '.join(df_fin.columns)
        values = ', '.join([':' + str(i + 1) for i in range(len(df_fin.columns))])
        table_name = table

        data = [tuple(row) for row in df_fin.values]
        insert_query = f"INSERT INTO {table_name} VALUES ({values})"

        if len(data) > partition_size:
            start_pos = 0
            partition_q = int(df_fin.shape[0] / partition_size)
            print(f'Se divide el dataframe en {partition_q} partes')
            while start_pos < len(data):
                data_part = data[start_pos:start_pos + partition_size]
                start_pos += partition_size
                try:
                    cursor.executemany(insert_query, data_part)
                except Exception as e:
                    print(f"Error al insertar el bloque desde {start_pos - partition_size} hasta {start_pos}")
                    print("Tipo de error:", type(e).__name__)
                    print("Detalle:", e)
                    for row in data_part:
                        try:
                            cursor.execute(insert_query, row)
                        except Exception as inner_e:
                            print("Error en fila individual:")
                            print("Fila:", row)
                            print("Tipo de error:", type(inner_e).__name__)
                            print("Detalle:", inner_e)
                            for i, value in enumerate(row):
                                try:
                                    # Intento de conversión individual para detectar el campo
                                    df_fin.iloc[0, i]  # Trigger para capturar el campo
                                except Exception as field_e:
                                    print(f"Posible error en campo '{df_fin.columns[i]}':", field_e)
                            raise
        else:
            try:
                cursor.executemany(insert_query, data)
            except Exception as e:
                print("Error al insertar el conjunto completo:")
                print("Tipo de error:", type(e).__name__)
                print("Detalle:", e)
                for row in data:
                    try:
                        cursor.execute(insert_query, row)
                    except Exception as inner_e:
                        print("Error en fila individual:")
                        print("Fila:", row)
                        print("Tipo de error:", type(inner_e).__name__)
                        print("Detalle:", inner_e)
                        for i, value in enumerate(row):
                            try:
                                df_fin.iloc[0, i]
                            except Exception as field_e:
                                print(f"Posible error en campo '{df_fin.columns[i]}':", field_e)
                        raise

        connection.commit()
        print(f"Se insertaron {len(data)} filas en la tabla {table_name}.")

    except cx_Oracle.DatabaseError as db_err:
        print("Error de base de datos:", db_err)
        traceback.print_exc()
    except Exception as e:
        print("Error general:", e)
        traceback.print_exc()
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if connection:
            try:
                connection.close()
            except:
                pass   