import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from controladores.empleado_controlador import EmpleadoControlador
from controladores.bd_controlador import BdControlador

class VistaEmpleado:

    ruta_input = ""
    ruta_log = ""

    def __init__(self,ruta_sistema):
        self.ruta_input = ruta_sistema / '../inputs/Empleados.txt'
        self.ruta_log = ruta_sistema / '../app/logs/Empleados.txt'
        self.empleado_controlador = EmpleadoControlador()
        self.bd_controlador = BdControlador()        

    def leer_informacion(ruta):
        try:
            lista_empleados = pd.read_csv(ruta,delimiter=",",header=None)
            lista_empleados = lista_empleados.values.tolist()
            return lista_empleados
        except Exception as e:
            print(f"Error: {e}")

    def guardar_logs(self,data):
        dataframe = pd.DataFrame(data,columns=["Fecha", "Mensaje"])
        dataframe.to_csv(self.ruta_log,index=False)

    def dividir_en_lotes(data, cantidad_lote):
        for i in range(0, len(data), cantidad_lote):
            yield data[i:i + cantidad_lote]
    

    def carga_empleados(self, numero_hilos = 200, cantidad_lotes = 2000):
        conexion, cursor = self.bd_controlador.iniciar_bd()
        lista_empleados = VistaEmpleado.leer_informacion(self.ruta_input)

        print(len(lista_empleados))

        lotes = list(VistaEmpleado.dividir_en_lotes(lista_empleados,cantidad_lotes))

        with ThreadPoolExecutor(max_workers=numero_hilos) as executor:
            executor.map(lambda lote: self.empleado_controlador.crear_empleado(conexion,cursor,lote), lotes)
        
        logs = self.empleado_controlador.generacion_logs(conexion,cursor)
        VistaEmpleado.guardar_logs(self,logs)
        
        self.bd_controlador.cerrar_bd(conexion,cursor)