/*
    Comments: Various columns can be added to this Dimensions table with a more clear understanding of what data to expect. Current design strictly kept limited based on incoming events data.
    
*/

CREATE TABLE public.services_dim
(
service_key integer NOT NULL,
service_name_nl character (50),
service_name_en character (50),
CONSTRAINT services_dim_pk PRIMARY KEY (service_key)
)
WITH (
OIDS=FALSE
);

COMMENT ON TABLE services_dim IS 'Services dimension table';
COMMENT ON COLUMN services_dim.service_key IS 'Services dimension PK';