from vistas.departamento_vista import VistaDepartamento
from vistas.empleado_vista import VistaEmpleado
from vistas.proyecto_vista import VistaProyecto
from pathlib import Path
import time

ruta_sistema = Path(__file__).parent


vista_d = VistaDepartamento(ruta_sistema)
vista_e = VistaEmpleado(ruta_sistema)
vista_p = VistaProyecto(ruta_sistema)


while True:

    print("1. Carga Masiva")
    print("2. Obtención Proyectos")
    print("3. Actualización Masiva")
    print("4. Salir")
    opcion = int(input("Seleccione una opción: "))

    if opcion == 1:
        print("Prueba")
        inicio = time.time()
        vista_d.carga_departamentos()
        vista_e.carga_empleados()
        vista_p.carga_proyectos()
        final = time.time()
        print(f"Tiempo transcurrido en segundos: {final - inicio}")
    elif opcion == 2:
        vista_p.buscar_totalidad_proyectos()
    elif opcion == 3:
        vista_p.carga_actualizacion_proyectos()
    elif opcion == 4:
        break
    else:
        print("Opción no válida.")
        break