/*
    Assumptions and Approach:
    
    1. Time dimension is broken down only to minutes since business does not require seconds level grain.
    2. Generic Day time indicators (Early Morning, Morning, Noon, Evening, and Night) are added; custom can be done.
*/

CREATE TABLE time_dim
(
time_key integer NOT NULL,
time_value character(5) NOT NULL,
hours_24 character(2) NOT NULL,
hours_12 character(2) NOT NULL,
hour_minutes character (2)  NOT NULL,
day_minutes integer NOT NULL,
day_time_name character varying (20) NOT NULL,
am_pm character(2) NOT NULL,
day_night character varying (20) NOT NULL,
CONSTRAINT time_dim_pk PRIMARY KEY (time_key)
)
WITH (
OIDS=FALSE
);

COMMENT ON TABLE time_dim IS 'Time Dimension';
COMMENT ON COLUMN time_dim.time_key IS 'Time Dimension PK';


insert into  time_dim
SELECT  cast(to_char(minute, 'hh24mi') as numeric) time_key,
to_char(minute, 'hh24:mi') AS time_value,
-- Hour of the day (0 - 23)
to_char(minute, 'hh24') AS hour_24,
-- Hour of the day (0 - 11)
to_char(minute, 'hh12') hour_12,
-- Hour minute (0 - 59)
to_char(minute, 'mi') hour_minutes,
-- Minute of the day (0 - 1439)
extract(hour FROM minute)*60 + extract(minute FROM minute) day_minutes,
	-- Names of day periods
 case
		when to_char( minute,
		'hh24:mi' ) between '05:30' and '08:29' then 'Early Morning'
		when to_char( minute,
		'hh24:mi' ) between '08:30' and '11:59' then 'Morning'
		when to_char( minute,
		'hh24:mi' ) between '12:00' and '16:29' then 'Noon'
		when to_char( minute,
		'hh24:mi' ) between '16:30' and '22:29' then 'Evening'
		else 'Night'
	end as day_time_name,
	-- Indicator of day or night
case
		when to_char( minute,
		'hh24:mi' ) between '00:00' and '11:59' then 'AM'
		else 'PM'
	end as am_pm,
 case
		when to_char( minute,
		'hh24:mi' ) between '07:00' and '19:59' then 'Day'
		else 'Night'
	end as day_night
from
	(
	select
		'0:00'::time + ( sequence.minute || ' minutes' )::interval as minute
	from
		generate_series( 0,
		1439 ) as sequence( minute )
	group by
		sequence.minute ) DQ
order by
	1;