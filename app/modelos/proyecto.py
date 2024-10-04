from threading import Lock


class Proyecto:

    
    def __init__(self,id,nombre,empleado_id):
        self.id = id
        self.nombre = nombre
        self.empleado_id = empleado_id 
        self.lock = Lock()
    

    def crear_proyecto(self,conexion,cursor,lote):
        with self.lock:
            try:
                datos = [(proyecto[0],proyecto[1],) for proyecto in lote]
                query = "INSERT INTO proyectos_temp (nombre,empleado) VALUES (%s,%s)"
                cursor.executemany(query,datos)
                conexion.commit()

            except Exception as e:
                print(f"Error: {e}")

    
    def genera_logs(self,cursor):
        try:
            query = "SELECT fecha, mensaje FROM proyectos_log"
            cursor.execute(query,())
            resultado = cursor.fetchall()

            return resultado
        except Exception as e:
            print(f"Error: {e}")

    def eliminar_info(self,conexion,cursor):
        try:
            query = "DELETE FROM proyectos_log"
            cursor.execute(query,())
            conexion.commit()
            
            query = "DELETE FROM proyectos_temp"
            cursor.execute(query,())
            conexion.commit()
    
        except Exception as e:
            print(f"Error: {e}")


    def buscar_totalidad_proyectos(self,cursor):
        try:
            query = "SELECT p.id, p.nombre, e.nombre, d.nombre FROM proyectos p INNER JOIN empleados e ON p.empleado_id = e.id INNER JOIN departamentos d ON e.departamento_id = d.id"
            cursor.execute(query,())
            resultado = cursor.fetchall()
            if resultado != None:
                return resultado
            else:
                return None
            
        except Exception as e:
            print(f"Error: {e}")


    def actualizar_nombre_proyectos(self,conexion,cursor,lote):
        with self.lock:
            try:
                lote = [(registro[2], registro[1], registro[0]) for registro in lote]
                query = "UPDATE proyectos SET nombre = %s WHERE nombre = %s AND empleado_id IN ( SELECT e.id FROM empleados e INNER JOIN departamentos d ON e.departamento_id = d.id WHERE d.nombre = %s)"
                cursor.executemany(query,lote)
                conexion.commit()

            except Exception as e:
                print(f"Error: {e}")

