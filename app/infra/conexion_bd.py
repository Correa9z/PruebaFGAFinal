import mysql.connector


class ConexionBd:


    def conectar_bd(self):
        try:
            conexion = mysql.connector.connect(host='localhost', port='3306',user='root',password='',database='pruebafga')

            cursor = conexion.cursor()

            return conexion, cursor

        except Exception as e:
            print("Error: ",e)
    
    def cerrar_bd(self,conexion,cursor):
        cursor.close()
        conexion.close()
