# Reglas de negocio

Fecha: July 14, 2022
Responsable: Anonymous
Semana: S2
Status: In Progress

1. Las unidades de taxis deben poder proporcionar el servicio de viaje a sus pasajeros.

La fecha y hora de **tpep_dropoff_datetime** no puede inferior al de **tpep_pickup_datetime**.

```python
if **tpep_dropoff_datetime** < **tpep_pickup_datetime** = DROP
```

Los valores en las distancias en **trip_distance** deben ser positivos.

```python
if **trip_distance < 0 = abs(trip_distance)**
```

1. El pasajero debe poder solicitar una unidad de taxi para su viaje.

Los valores en la tarifa de **fare_amount** deben ser positivos.

```python
if **fare_amount** < 0 = abs(**fare_amount**)
```

1. Los viajes deben suceder en zonas y barrios existentes.

Las ubicaciones de subida **pu_location** y bajada **do_location** de cada viaje deben pertenecer a una ubicación en la tabla **location**.

```python
**taxis**[**pu_location**] is not **location**[**id_location] =** DROP
**taxis**[**do_location**] is not **location**[**id_location] =** DROP
```

1. Las tarifas son establecidas por un código dado por la locación final del viaje.

El código de tarifa de cada viaje **ratecodeid** debe estar en un rango del 1 al 6.

```python
if **ratecodeid** < 1 and **ratecodeid >** 6 = DROP
```

1. El pasajero puede decidir su método de pago preferente.

El código de pago de cada viaje **id_payment** debe estar en un rango del 1 al 6. 

```python
if **id_payment** < 1 and **id_payment >** 6 = DROP
```

1. Cada viaje genera una ganancia dependiendo de la tarifa dada.

El valor en **total_amount** debe ser mayor o igual que en **fare_amount**.

```python
if **total_amount < fare_amount =** DROP
```