from modelos.departamento import Departamento

class DepartamentoControlador:

    def __init__(self):
        self.departamento = Departamento("","")

    def crear_departamento(self,conexion,cursor,lote):
        self.departamento.crear_departamentos(conexion,cursor,lote)
        
    def generacion_logs(self,conexion,cursor):
        logs = self.departamento.genera_logs(cursor)
        self.departamento.eliminar_info(conexion,cursor)
        return logs