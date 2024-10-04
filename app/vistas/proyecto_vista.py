import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from controladores.proyecto_controlador import ProyectoControlador
from controladores.bd_controlador import BdControlador
import logging
import os

class VistaProyecto:

    ruta_input = ""
    ruta_log = ""
    ruta_input_actualizacion = ""
    ruta_totalidad_proyectos = ""
    logger = ""

    def __init__(self,ruta_sistema):
        self.ruta_input = ruta_sistema / '../inputs/Proyectos.txt'
        self.ruta_log = ruta_sistema / '../app/logs/Proyectos.txt'
        self.ruta_input_actualizacion = ruta_sistema / '../inputs/ProyectosActualizacion.txt'
        self.ruta_totalidad_proyectos = ruta_sistema / '../resultados/ProyectosTotalidad.xlsx'
        self.proyecto_controlador = ProyectoControlador()
        self.bd_controlador = BdControlador()  


    def leer_informacion(ruta):
        try:
            lista_proyectos = pd.read_csv(ruta,delimiter=",",header=None)
            lista_proyectos = lista_proyectos.values.tolist()
            return lista_proyectos
        except Exception as e:
            print(f"Error: {e}")


    def guardar_logs(self,data):
        for dato in data:
            partes = dato[0].split(",")
            if(partes[0] == "Error"):
                self.logger.error(partes[1])
            else:
                self.logger.info(partes[1])


    def dividir_en_lotes(data, cantidad_lote):
        for i in range(0, len(data), cantidad_lote):
            yield data[i:i + cantidad_lote]


    def buscar_totalidad_proyectos(self):
        conexion, cursor = self.bd_controlador.iniciar_bd()
        resultados = self.proyecto_controlador.buscar_totalidad_proyectos(cursor)
        dataframe = pd.DataFrame(resultados,columns=['Id Proyecto', 'Nombre Proyecto', 'Nombre Empleado', 'Departamento Nombre'])
        dataframe.to_excel(self.ruta_totalidad_proyectos,index=False)
        self.bd_controlador.cerrar_bd(conexion,cursor)


    def carga_proyectos(self, numero_hilos = 20, cantidad_lotes = 2000):
        conexion, cursor = self.bd_controlador.iniciar_bd()
        lista_proyectos = VistaProyecto.leer_informacion(self.ruta_input)
        print(len(lista_proyectos))
        lotes = list(VistaProyecto.dividir_en_lotes(lista_proyectos,cantidad_lotes))

        with ThreadPoolExecutor(max_workers=numero_hilos) as executor:
            executor.map(lambda lote: self.proyecto_controlador.crear_proyecto(conexion,cursor,lote), lotes)
        
        logs = self.proyecto_controlador.generacion_logs(conexion,cursor)
        VistaProyecto.iniciar_logs(self)
        VistaProyecto.guardar_logs(self,logs)
        
        self.bd_controlador.cerrar_bd(conexion,cursor)
    

    def carga_actualizacion_proyectos(self, numero_hilos = 20, cantidad_lotes = 2000):
        conexion, cursor = self.bd_controlador.iniciar_bd()
        lista_proyectos = VistaProyecto.leer_informacion(self.ruta_input_actualizacion)

        lotes = list(VistaProyecto.dividir_en_lotes(lista_proyectos,cantidad_lotes))

        with ThreadPoolExecutor(max_workers=numero_hilos) as executor:
            executor.map(lambda lote: self.proyecto_controlador.actualizar_proyectos(conexion,cursor,lote), lotes)
        self.bd_controlador.cerrar_bd(conexion,cursor)


    def iniciar_logs(self):
        logger = logging.getLogger('proyecto')
        logger.setLevel(logging.DEBUG)
        
        if not logger.hasHandlers():
            file_handler = logging.FileHandler(os.path.join("app/logs/", 'proyecto.log'))
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        self.logger = logger