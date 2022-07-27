# Arquitectura del proyecto Taxis NYC & WEATHER API
<br>

![Stack tecno](../_src/Linode_-_Cloud_.png)

<br>

## Orquestación
Para el proceso de carga de los meses sucesivos hemos decidido usar Airflow, ya que con esta tecnologia podemos ejecutar  paso a paso y en intervalos de tiempo, la extraccion, carga y transformacionn de los datos.
<br>

Ademas crearemos una base de datos en postgresql que conectaremos con airflow. 
<br>

Paso 1: Creamos el esquema.
<br>

Paso 2: Cargamos las tablas Vendor, Rate, Payment, Borough y Location (Tablas que seran creadas y cargadas una sola vez)

<br>

![Tarifas - Propinas](../_src/airflow1.jpeg)

![Tarifas - Propinas](../_src/airflow2.jpeg)

<br>
Paso 3: Luego se ejecutan dos procesos el ETL_taxi_trips y ETL_weather.
<br>

![Tarifas - Propinas](../_src/airflow3.jpeg)

Paso 4: El proceso ETL_taxis_trips cuenta con 3 etapas, extraccion de los datos desde la pagina (extrac_trip), transformacion de los datos con python (transform_trip) y la carga a los datos a la base en postgressql (load_trip).

<br>

![Tarifas - Propinas](../_src/airflow4.jpeg)

Paso 5: El proceso ETL_weather cuenta con 2 etapas, extraccion y transformacion de los datos desde la pagina (extrac_transform_weather)y la carga a los datos a la base en postgressql (load_weather).

<br>

![Tarifas - Propinas](../_src/airflow5.jpeg)

<br>

![Tarifas - Propinas](../_src/airflow6.jpeg)

<br>

![Tarifas - Propinas](../_src/airflow7.jpeg)

<br>


# Datawarehouse
A la hora de armar la base de datos, decidimos trabjar con postgresql.
Con postgresql armamos la base de datos con sus tablas, llaves primarias y llaves foraneas. Utilizaremos Airflow para orquestar la automatizacion del pipeline ETL, como tambien para la creacion de las tablas.

<br>

## Creacion de la base de datos
Trabajamos con la tabla __trip_data__ que tendra todos los datos obtenidos del dataset Taxis.csv, tendra una primarykey llamada "id_trip_data".
trip_data cuenta con 5 llavaes foraneas:
<br>
- __Rate:__ Entre id_date_code de la tabla trip_data y id_rate de la tabla rate. Contalbiliza los tipos de tarifa de los viajes
- __Pay_tipe:__ Entre id_pay_tipe de la tabla trip data y id_pay_tipe de la tabla pay_tipe. Contabiliza los metodos de pagos de los usuarios.
- __Vendor:__ Entre id_vendor de la tabla trip data y id_vendor de la tabla vendor. Contabiliza la empresa que realizo el viaje.
- __Localtion:__ Con esta tabla tenemos una doble llave foranea. La primera para la columna pu_location y la segunda para do_location ambas de la tabla trip_data y se conectan con la columna id_location de la tabla location. En estas columnas estamos mostrando lo localizacion del taxi en el inicio del viaje y la localizacion en su final.

<br>

Para continuar tenemos la tabla __weather__, esta tabla cuenta con una llave primaria llamada id_weather, que informa sobre cada una de las situaciones climaticas en los lugares correspondientes en un periodo de una hora. Ademas cuenta con 1 llave foranea:
<br>
- __Borough:__ La columna id_borough de la tabla weather con la tabla id_borough de la tabla borough. Para coordinar la ubicacion de cada situacion climatica registrada en su tabla correspondiente.


Terminando el analisis de las llaves foraneas tenemos la tabla __borough__, donde la llave primaria es el id_borough y posee una llave foranea:
<br>
- __Location:__ La id_borough de la tabla borough con la columna id_borough de la tabla location. Para coordinar las jerarquias entre los borough y las zonas.

<br>

Ademas tenemos las tablas __calendario__ donde la clave primaria es la fecha, y tenemos las columnas sobre dia, semana, mes, trimestre, etc.

La tabla __location__ donde organizamos las jerarquias y las relaciones entre las zonas y los borough.

La tabla __rate__ donde anotamos los tipos de tarifas, la tabla __pay_tipe__ y la tabla __vender__ donde vemos la empresa que realizo el viaje.

<br>

![Tarifas - Propinas](../_src/unknown.png)

<br>
