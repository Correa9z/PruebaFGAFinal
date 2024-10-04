from threading import Lock

class Departamento:
    

    def __init__(self,id,nombre):
        self.id = id
        self.nombre = nombre 
        self.lock = Lock()
    

    def crear_departamentos(self,conexion,cursor,lote):
        with self.lock:
            try:
                datos = [(departamento[0],) for departamento in lote]
                query = "INSERT INTO departamentos_temp (nombre) VALUES (%s)"
                cursor.executemany(query,datos)
                conexion.commit()

            except Exception as e:
                print(f"Error: {e}")

    def genera_logs(self,cursor):
        try:
            query = "SELECT fecha, mensaje FROM departamentos_log"
            cursor.execute(query,())
            resultado = cursor.fetchall()

            return resultado
        except Exception as e:
            print(f"Error: {e}")

    def eliminar_info(self,conexion,cursor):
        try:
            query = "DELETE FROM departamentos_log"
            cursor.execute(query,())
            conexion.commit()
            
            query = "DELETE FROM departamentos_temp"
            cursor.execute(query,())
            conexion.commit()
    
        except Exception as e:
            print(f"Error: {e}")
