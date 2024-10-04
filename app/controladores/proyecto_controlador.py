from modelos.proyecto import Proyecto

class ProyectoControlador:

    def __init__(self):
        self.proyecto = Proyecto("","","")

    def crear_proyecto(self,conexion,cursor,lote):
        self.proyecto.crear_proyecto(conexion,cursor,lote)

    def generacion_logs(self,conexion,cursor):
        logs = self.proyecto.genera_logs(cursor)
        self.proyecto.eliminar_info(conexion,cursor)
        return logs

    def buscar_totalidad_proyectos(self,cursor):
        return self.proyecto.buscar_totalidad_proyectos(cursor)
    
    def actualizar_proyectos(self,conexion,cursor,lote):
        self.proyecto.actualizar_nombre_proyectos(conexion,cursor,lote)
