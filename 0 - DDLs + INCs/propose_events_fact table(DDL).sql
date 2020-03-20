/*
    Comments: Various columns can be added to this fact table with a more clear understanding of what data to expect and metrics. Current design strictly kept limited based on incoming events data.
    
    This is our main fact table in star schema. Intentionally chose not to add Foreign Key constraints as we mostly turn them OFF on bulk load anyway.
    
    Kept a composite primary key with basic 3 elements.
*/

CREATE TABLE public.propose_events_fact
(
event_id_key integer NOT NULL,
event_type_key integer NOT NULL,
professional_key integer NOT NULL,
service_key integer NOT NULL,
date_key integer NOT NULL,
time_key integer NOT NULL,
lead_fee double precision NOT NULL,
CONSTRAINT propose_events_fact_pk PRIMARY KEY (event_id_key, professional_key, date_key)
)
WITH (
OIDS=FALSE
);

COMMENT ON TABLE propose_events_fact IS 'Propose Events Fact table';
