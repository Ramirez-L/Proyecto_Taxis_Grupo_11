# Documentación API

## Descripción

La siguiente es la documentación de la conexión hacia la base de datos a traves de una API.

Ha sido creada con un backend en Python, con la librería FastAPI y el código se encuentra en el archivo __consulta.py__

Las rutas o endpoints para poder acceder a los dato son:

- GET: URL/query/

Esta ruta es para hacer el envío de la consulta hacia la base de datos, solo permite opciones de SELECT, no estan permitidas otras opciones que puedan alterar los datos.

- GET: URL/model

Haciendo el llamado a esta dirección, se puede descargar el archivo .SAV del modelo de datos aplicado en la página [Demostración consumible del modelo de Machine Learning](https://mangoru-taxi-trips-amount-prediction-streamlit-app-0z2yr7.streamlitapp.com/)

## Pasos para iniciar el Servidor

Primero realizar la instalación de las dependencias en la PC o Computador que será el nuevo servidor desde la dirección donde se encuentra el archivo __requirements.txt__ para efectos del proyecto se encuentra en una máquina distinta a la usada en el proyecto.

``` python
pip install -r requirements.txt
```

O usar:

``` python
pip3 install -r requirements.txt
```

Depende de la versión pip instalada en el servidor.

Luego ir hacia la ruta donde se encuentre el archivo __consulta.py__

``` python
cd /[directorio]/
```

Finalmente, iniciar el servidor por medio de CMD

``` cmd
uvicorn [nombre_archivo_sin_py]:[nombre_del_objeto_en_el_archivo] --host [URL_maquina] --port [puerto_a_usar]
```

Ejemplo, como ha sido usado en el proyecto

``` cmd
uvicorn consulta:app --host 192.53.165.168 --port 8000
```

## Return o Respuestas de las llamadas a la API

- GET: URL/query/

Devuelve los datos en un Array (Javascript) o Lista (Python) con cada resultado de la consulta formato JSON de la forma:

``` javascript
[
    {'Columna': "Fila"},
    {'Columna': "Fila"},
    {'Columna': "Fila"},
    ...
]
```

Ejemplo:

``` SQL
SELECT COUNT(*) from trip
```

``` javascript
[
    {'count': 99999999}
]
```

- GET: URL/model

Ruta para poder descargar el modelo de Machine Learning desde el navegador. Regresa el archivo .sav
