/*
    Comments: Various columns can be added to this Dimensions table with a more clear understanding of what data to expect. Current design strictly kept limited based on incoming events data.
    
    Logic: Only gets populated upon receiving of a 'created_account' event. This logic helps in identifying the 'PROFESSIONALS' discrepency between source system and DWH. Event date is used as 'Account Creation Date'. "professional_id_anonymized" was converted to professional_key as it is already a surrogate column.
*/

CREATE TABLE public.professionals_dim
(
professional_key integer NOT NULL,
professional_id integer NOT NULL,
acc_creation_date DATE NOT NULL,
CONSTRAINT professionals_dim_pk PRIMARY KEY (professional_key)
)
WITH (
OIDS=FALSE
);

COMMENT ON TABLE professionals_dim IS 'Professionals dimension table';
COMMENT ON COLUMN professionals_dim.professional_key IS 'Professionals dimension PK';
