import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from controladores.departamento_controlador import DepartamentoControlador
from controladores.bd_controlador import BdControlador


class VistaDepartamento:

    ruta_input = ""
    ruta_log = ""

    def __init__(self,ruta_sistema):
        self.ruta_input = ruta_sistema / '../inputs/Departamentos.txt'
        self.ruta_log = ruta_sistema / '../app/logs/Departamentos.txt'
        self.departamento_controlador = DepartamentoControlador()
        self.bd_controlador = BdControlador()  

    def leer_informacion(ruta):
        try:
            lista_departamentos = pd.read_csv(ruta,header=None)
            lista_departamentos = lista_departamentos.values.tolist()
            return lista_departamentos
        except Exception as e:
            print(f"Error: {e}")

    def guardar_logs(self,data):
        dataframe = pd.DataFrame(data,columns=["Fecha", "Mensaje"])
        dataframe.to_csv(self.ruta_log,index=False)

    def dividir_en_lotes(data, cantidad_lote):
        for i in range(0, len(data), cantidad_lote):
            yield data[i:i + cantidad_lote]

    def carga_departamentos(self, numero_hilos=20, cantidad_lotes=2000):
        conexion, cursor = self.bd_controlador.iniciar_bd()
        lista_departamentos = VistaDepartamento.leer_informacion(self.ruta_input)

        print(len(lista_departamentos))

        lotes = list(VistaDepartamento.dividir_en_lotes(lista_departamentos,cantidad_lotes))

        with ThreadPoolExecutor(max_workers=numero_hilos) as executor:
            executor.map(lambda lote: self.departamento_controlador.crear_departamento(conexion,cursor,lote), lotes)

        logs = self.departamento_controlador.generacion_logs(conexion,cursor)
        VistaDepartamento.guardar_logs(self,logs)

        self.bd_controlador.cerrar_bd(conexion,cursor)