CREATE TABLE airflow.daily(
	sunrise date,
	sunset date,
	temperature float4,
	pressure int,
	humidity int,
	clouds int,
	wind_speed float4,
	rain float4,
	snow float4
);

CREATE TABLE airflow.historic(
	dt date,
	pressure int,
	humidity int,
	clouds int,
	temperature int,
	wind_speed float4,
	wind_direction int,
	weather_conditions varchar(255),
	rain float4
);