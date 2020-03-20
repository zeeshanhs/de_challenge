/*
    Comments: Various columns can be added to this snapshot style fact table with a more clear understanding of what data to expect. Current design strictly kept limited based on incoming events data.
*/

CREATE TABLE public.events_fact
(
event_id_key integer NOT NULL,
event_type_key integer NOT NULL,
professional_key integer NOT NULL,
date_key integer NOT NULL,
time_key integer NOT NULL,
CONSTRAINT events_fact_pk PRIMARY KEY (event_id_key)
)
WITH (
OIDS=FALSE
);

COMMENT ON TABLE events_fact IS 'Events Fact table';
COMMENT ON COLUMN events_fact.event_id_key IS 'Events Fact PK';