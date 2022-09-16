CREATE TABLE tfm.tramos3 (
IdConductor text,
IdVehiculo text,
FechaInicio timestamp,
FechaFinal timestamp,
Distancia double,
Velocidad double,
TramoData map<text,double>,
PRIMARY KEY ((IdVehiculo, IdConductor),FechaInicio));
