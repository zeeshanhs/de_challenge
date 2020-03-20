/*
    Comments: Various columns can be added to this 'Event Type' Dimensions table with a more clear understanding of what data to expect. Current design strictly kept limited based on incoming events data.
    
    Logic: Due to it being central table, it is possible to generate it at start and add any changes to table through a separate ETL or change-management.
    
*/

CREATE TABLE public.event_type_dim
(
event_type_key integer NOT NULL,
event_type character (50) NOT NULL,
CONSTRAINT event_type_dim_pk PRIMARY KEY (event_type_key)
)
WITH (
OIDS=FALSE
);

COMMENT ON TABLE event_type_dim IS 'Event Type dimension table';
COMMENT ON COLUMN event_type_dim.event_type_key IS 'Event Type dimension PK';


insert into event_type_dim

select 0, 'created_account'
union all
select 1, 'became_able_to_propose'
union all
select 2, 'became_unable_to_propose'
union all
select 3, 'proposed';