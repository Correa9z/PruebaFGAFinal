from infra.conexion_bd import ConexionBd

class BdControlador:

    def __init__(self):
        self.conexion_bd = ConexionBd()

    def iniciar_bd(self):
        conexion, cursor = self.conexion_bd.conectar_bd()
        return conexion, cursor

    def cerrar_bd(self,conexion,cursor):
        if (cursor != None):
            self.conexion_bd.cerrar_bd(conexion,cursor)