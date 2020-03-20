/*
    Comments: Various columns can be added to this EVENT_ID Dimensions table with a more clear understanding of what data to expect. Current design strictly kept limited based on incoming events data.
    
*/

CREATE TABLE public.event_id_dim
(
event_id_key integer NOT NULL,
CONSTRAINT event_id_dim_pk PRIMARY KEY (event_id_key)
)
WITH (
OIDS=FALSE
);

COMMENT ON TABLE event_id_dim IS 'Event_ID dimension table';
COMMENT ON COLUMN event_id_dim.event_id_key IS 'Event_ID dimension PK';