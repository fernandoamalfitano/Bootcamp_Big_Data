--PRÁCTICA DATA WAREHOUSE & SQL: FERNANDO AMALFITANO

--Enunciado 1:
select 
	count(flight_row_id) as number_of_records,
	count(distinct unique_identifier) as different_flights
from flights;	

--1) Hay 1209 registros.
--2) Hay 266 vuelos distintos.

with different_identifier as (
	select 
		unique_identifier as identifier,
		count(unique_identifier) as number_of_records
	from flights
	group by 1
	having count(unique_identifier) > 1
)
select 
	count(identifier) as more_than_one_record
from different_identifier;

--3) Hay 250 vuelos que tienen más de un registro.
--------------------------------------------------------------------------------------------------------------------
--Enunciado 2:
select 
	*
from flights
where unique_identifier in ('AA-102-20241001-JFK-MAD','BA-287-20250524-LHR-MAD');

-- Puedo deducir que hay varios registros de 'unique_identifier' debido a actualizaciones en la información del vuelo.
-- La columna 'updated_at' nos indica en que momento se ha realizado la actualización de la información.
-- La información que cambia es la hora de despegue y aterrizaje en destino del avión, debido a retraso o demora por ejemplo.
-- También se va actualizando la columna 'delay' que nos indica en minutos la demora que tiene ese vuelo.

---------------------------------------------------------------------------------------------------------------------
--Enunciado 3:
select 
	unique_identifier,
	count(distinct created_at) as numbers_of_created_at
from flights
group by 1
having count(distinct created_at) > 1;
--1) Cada vuelo tiene un único 'created_at', hay algunos vuelos que tienen el valor NULL pero lo manteniene en todos sus diferentes registros.

select
	unique_identifier,
	created_at,
	updated_at
from flights
where created_at > updated_at;

--2) En ningún resgistro el 'created_at' es mayor que 'updated_at', esto nos indica que los datos son coherentes y consistentes.

---------------------------------------------------------------------------------------------------------------------
--Enunciado 4:

with base as(
	select 
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
), 
last_records as ( 
	select *
	from base 	
	where rn = 1
)

select *
from last_records;

---------------------------------------------------------------------------------------------------------------------
--Enunciado 5:

with base as(
	select 
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
), 
last_records as ( 
	select *
	from base 	
	where rn = 1
),
clean_records as (
	select 
		*,
		case 
			when local_departure is null then created_at
			else local_departure
		end as effective_local_departure,
		case 
			when local_departure is null and local_actual_departure is null then created_at
			when local_actual_departure is null then local_departure
			else local_actual_departure
		end as effective_local_actual_departure
	from last_records
)

select 
	* 
from clean_records;

---------------------------------------------------------------------------------------------------------------------
--Enunciado 6:

with base as(
	select 
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
), 
last_records as ( 
	select *
	from base 	
	where rn = 1
)

select 
	arrival_status,
	count(unique_identifier) as number_of_status	
from last_records
group by 1;

-- Hay 6 estados de vuelo: CX, DY, EY, NS, OT  y NULL.
-- Los estados que mas se repiten son 'vuelo demorado' y 'vuelo a tiempo'. 

-- CX: Vuelo cancelado
-- DY: Vuelo demorado
-- EY: Vuelo adelantado
-- NS: No programado
-- OT: Vuelo a tiempo o puntual
-- NULL: Sin datos

--------------------------------------------------------------------------------------------------------------------
--Enunciado 7:

with base as(
	select 
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
), 
last_records as ( 
	select *
	from base 	
	where rn = 1
),
info_country as (

select 
	flight_row_id,
	unique_identifier,
	departure_airport,
	case 
		when departure_airport = 'CDG' then 'France'
		when departure_airport = 'MUC' then 'Germany'
		when departure_airport = 'FCO' then 'Italy'
		when departure_airport = 'NRT' then 'Japan'
		when departure_airport = 'AMS' then 'Netherlands'
		when departure_airport = 'BCN' then 'Spain'
		when departure_airport = 'MAD' then 'Spain'
		when departure_airport = 'LHR' then 'United Kingdom'
		when departure_airport = 'JFK' then 'United States'
		when departure_airport = 'SFO' then 'United States'

	end as country_airport		
from last_records
)

select	
	count(unique_identifier),
	country_airport
	
from info_country
group by 2;

-- Despegan de los paises que salen en la query anterior. También nos encontramos con que muchos vuelos con valor NULL
-- en aeropuerto de despegue. Se podria analizar las siglas y asignar el país correspondiente. En este caso lo dejaré como NULL, ya que no
-- aparecen en el csv de airports.

-------------------------------------------------------------------------------------------------------------------
--Enunciado 8:

with base as(
	select 
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
), 
last_records as ( 
	select *
	from base 	
	where rn = 1
),
info_country_delay_status as (
	select 
		departure_airport,
		arrival_status,
		delay_mins,
		case 
			when departure_airport = 'CDG' then 'France'
			when departure_airport = 'MUC' then 'Germany'
			when departure_airport = 'FCO' then 'Italy'
			when departure_airport = 'NRT' then 'Japan'
			when departure_airport = 'AMS' then 'Netherlands'
			when departure_airport = 'BCN' then 'Spain'
			when departure_airport = 'MAD' then 'Spain'
			when departure_airport = 'LHR' then 'United Kingdom'
			when departure_airport = 'JFK' then 'United States'
			when departure_airport = 'SFO' then 'United States'

		end as country_airport		
	from last_records
)

--1) Tiempo medio en minutos del delay por paises.
select 
	country_airport,
	round(avg(delay_mins),2) as average_delay_min
from info_country_delay_status
group by 1;

--2) Distribución de estados de vuelos por paises.
select 
	*
from info_country_delay_status;

------------------------------------------------------------------------------------------------------------------
--Enunciado 9:

with base as(
	select 
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
), 
last_records as ( 
	select *
	from base 	
	where rn = 1
),
clean_records as (
	select 
		*,
		case 
			when local_departure is null then created_at
			else local_departure
		end as effective_local_departure,
		case 
			
			when local_departure is null and local_actual_departure is null then created_at
			when local_actual_departure is null then local_departure
			else local_actual_departure
		end as effective_local_actual_departure
	from last_records
),
info_country_delay_status as (
	select 
		departure_airport,
		arrival_status,
		delay_mins,
		extract (month from effective_local_departure) as month_of_the_flight,
		

		case 
			when departure_airport = 'CDG' then 'France'
			when departure_airport = 'MUC' then 'Germany'
			when departure_airport = 'FCO' then 'Italy'
			when departure_airport = 'NRT' then 'Japan'
			when departure_airport = 'AMS' then 'Netherlands'
			when departure_airport = 'BCN' then 'Spain'
			when departure_airport = 'MAD' then 'Spain'
			when departure_airport = 'LHR' then 'United Kingdom'
			when departure_airport = 'JFK' then 'United States'
			when departure_airport = 'SFO' then 'United States'
		end as country_airport
		
	from clean_records
),
final as (
	select 
		departure_airport,
		country_airport,
		arrival_status,
		round(avg(delay_mins), 2) as avg_delay,
		case 
			when month_of_the_flight in (12,1,2) then 'Invierno'
			when month_of_the_flight in (3,4,5) then 'Primavera'
			when month_of_the_flight in (6,7,8) then 'Verano'
			when month_of_the_flight in (9,10,11) then 'Otoño'
		end as time_of_year
	from info_country_delay_status
	group by 1,2,3,5
	order by country_airport
)

select
	country_airport,
	SUM(avg_delay),
	time_of_year
	
from final
group by 1,3
order by 1,3;

-------------------------------------------------------------------------------------------------------------
--Enunciado 10:

with base as(
	select 
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
		
	from flights
),
function_lag as (
	select 
		*,
		lag(updated_at) over(partition by unique_identifier order by updated_at desc) as update_frequency
	from base
)
select 
	unique_identifier,
	rn,
	update_frequency - updated_at as diff
from function_lag;

--La actualización de la información de vuelo se realiza cada 6 horas.

----------------------------------------------------------------------------------------------------------------------
--Enunciado 11:

with base as(
	select 
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
), 
last_records as ( 
	select *
	from base 	
	where rn = 1
),
clean_information as (
	SELECT
		unique_identifier, 
		departure_airport,
		arrival_airport,
		(local_departure::date) as local_departure,
		split_part(unique_identifier,'-',1) as new_airline_code,
		split_part(unique_identifier,'-',2) as new_flight_number,
		split_part(unique_identifier,'-',3)::date as new_flight_date,
		split_part(unique_identifier,'-',4) as new_departure_airport,
		split_part(unique_identifier,'-',5) as new_destination_airport
	from last_records
),
unite_airlines as (
	select 
		cle.unique_identifier,
		cle.departure_airport,
		cle.arrival_airport,
		cle.local_departure,
		cle.new_airline_code,
		cle.new_flight_number,
		cle.new_flight_date,
		cle.new_departure_airport,
		cle.new_destination_airport,
		air.airline_code,
		air.name
	from clean_information as cle 
	left join airlines as air 
	on cle.new_airline_code=air.airline_code
),
flag as (
	select 
		*,
		case 
			when departure_airport = new_departure_airport and arrival_airport = new_destination_airport and local_departure = new_flight_date
			then 'consistent'
			else 'no_consistent'
			
		end as is_consistent
	from unite_airlines
)


--1)
select * 
from flag;

--2)
select 
	count(unique_identifier) as number_of_flights
from flag
where is_consistent = 'no_consistent';

--3)
select
	name,
	count(is_consistent) as number_no_consistent
from flag
where is_consistent = 'no_consistent'
group by 1;

-------------------------------------------------------------------------------------------------------------------------------------------------


	














