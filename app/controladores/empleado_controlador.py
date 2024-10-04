from modelos.empleado import Empleado

class EmpleadoControlador:

    def __init__(self):
        self.empleado = Empleado("","","","")

    def crear_empleado(self,conexion,cursor,lote):
        self.empleado.crear_empleado(conexion,cursor,lote)
    

    def generacion_logs(self,conexion,cursor):
        logs = self.empleado.genera_logs(cursor)
        self.empleado.eliminar_info(conexion,cursor)
        return logs
