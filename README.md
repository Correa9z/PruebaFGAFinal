![Logo FGA](https://fga.com.co/wp-content/uploads/2024/04/Logo-FGA-validado.png)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen)

# PruebaTFGA

## Descripción
Esta prueba técnica se encuentra basada en la implemenetación de soluciones optimas que permitan realizar distintas
acciones tales como cargue y actualización masivo, además de busquedas con respecto a la información.

## Instalación
1. Se debe de descargar o clonar el repositorio.
2. Instala las dependencias con `pip install -r` y la ruta en la que se encuentra el proyecto y el archivo requirements.txt.
3. Ejecutar el proyecto por medio del inicio a la carpeta app y al archivo app.py.

## Configuración Base de Datos
1. Se debe de dirigir hacia la ruta del proyecto infra/conexion_bd.py
2. Se debe de referenciar la linea de codigo ```mysql.connector.connect(host='localhost', port='3306',user='root',password='',database='pruebafga')``` 
3. Se debe de actualizar los argumentos de la función como el host, port, user, password y claramente el database.