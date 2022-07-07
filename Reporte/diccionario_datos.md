| Columna               | Tipo de Dato (Pandas) | Descripción                                                                                      |   |   |
|-----------------------|:-----------------------:|--------------------------------------------------------------------------------------------------|---|---|
| VendorID              |         int64         | Compañía de Servicios: 1=Creative Mobiles Technologies 2=VeriFone Inc.                           |   |   |
| tpep_pickup_datetime  |     datetime64[ns]    | Fecha y hora de inicio del Viaje                                                                 |   |   |
| tpep_dropoff_datetime |     datetime64[ns]    | Fecha y hora del final del Viaje                                                                 |   |   |
| passenger_count       |         int64         | Cantidad de Pasajeros                                                                            |   |   |
| trip_distance         |        float64        | Distancia en Millas por el taximetro                                                             |   |   |
| RatecodeID            |         int64         | Código de Tarifas                                                                                |   |   |
| store_and_fwd_flag    |         object        | Si el Taxi realizó la carga de datos en el vehículo o no                                         |   |   |
| PULocationID          |         int64         | Identificación de la Localidad de Inicio del Viaje                                               |   |   |
| DOLocationID          |         int64         | Identificación de la Localidad del Final del Viaje                                               |   |   |
| payment_type          |         int64         | Método de pago.  1=Tarjeta de crédito 2=Efectivo 3=Sin Cargo 4=Disputado 5=Desconocido 6=Anulado |   |   |
| fare_amount           |        float64        | Tarifa por tiempo y distancia del taximetro                                                      |   |   |
| extra                 |        float64        | Cargos extras por horas pico y horas nocturnas de $0.5 a $1                                      |   |   |
| mta_tax               |        float64        | Impuesto de $0.5 por tasa de metros                                                              |   |   |
| tip_amount            |        float64        | Propinas con Tarjetas de Crédito                                                                 |   |   |
| tolls_amount          |        float64        | Total de Peajes pagados en el viaje                                                              |   |   |
| improvement_surcharge |        float64        | Recargo para mejorar el servicio de $0.3 en la bajada de bandera                                 |   |   |
| total_amount          |        float64        | Cobro total a los pasajeros sin incluir propinas                                                 |   |   |
| congestion_surcharge  |        float64        | Cargo adicional por Tráfico                                                                      |   |   |
| Tiempo_Viaje_s        |        float64        | Tiempo total del viaje en Segundos                                                               |   |   |