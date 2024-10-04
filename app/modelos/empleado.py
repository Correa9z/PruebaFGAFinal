from threading import Lock


class Empleado:


    def __init__(self,id,nombre,identificacion,departamento_id):
        self.id = id
        self.nombre = nombre
        self.identificacion = identificacion
        self.departamento_id = departamento_id
        self.lock = Lock() 
    

    def crear_empleado(self,conexion,cursor,lote):
        with self.lock:
            try:
                datos = [(empleado[0],empleado[1],empleado[2],) for empleado in lote]
                query = "INSERT INTO empleados_temp (nombre,identificacion,departamento) VALUES (%s,%s,%s)"
                cursor.executemany(query,datos)
                conexion.commit()

            except Exception as e:
                print(f"Error: {e}")


    def genera_logs(self,cursor):
        try:
            query = "SELECT fecha, mensaje FROM empleados_log"
            cursor.execute(query,())
            resultado = cursor.fetchall()

            return resultado
        except Exception as e:
            print(f"Error: {e}")

    def eliminar_info(self,conexion,cursor):
        try:
            query = "DELETE FROM empleados_log"
            cursor.execute(query,())
            conexion.commit()
            
            query = "DELETE FROM empleados_temp"
            cursor.execute(query,())
            conexion.commit()
    
        except Exception as e:
            print(f"Error: {e}")
