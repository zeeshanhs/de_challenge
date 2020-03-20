import csv
import time
import pandas as pd
import re
import logging
import psycopg2
import psycopg2.extras
import datetime


from pprint import pprint
from IPython.display import display
from datetime import datetime, timedelta
from psycopg2.extensions import AsIs

"""

# Exploratory data Analysis stats (of given data)
  
  1. NULL count check:
      event_id : NULL count = 0
      event_type : NULL count = 0
      professional_id : NULL count = 0
      pid_anonymized : NULL count = 0
      created_at : NULL count = 0
      meta_data : NULL count = 2505

### Conclusion: Current data has no NULL inconsistencies.
  
  2. Data column having incorrect data-type value (handled in ETL)
  3. NAN count
  4. Suspecious data. (duplicate timestamp) - Found None
  5. Duplicate records where they shouldn't exist - Some cases handled in ETL.
  

"""



"""
    # Read data chunks from File

    The input data is rather simple, use CSV reader because it is better fit for our case of highly structured data and basic format.
    * Performance wise, very comparable to READ() method which reads a chunk into string.

    ### Handled cases during read:

    1. single or multiple empty lines in input data. (lines are simply skipped)
    2. If last column (meta_data) is missing instead of NULL, meta_data will have value as 'None'. (Invalid data : This row will be ingnored)
    3. If any one or more in-between column is missing or missing delimeter, there will be 1 or more 'None' values in row. (Invalid data : This row will be ingnored)
    4. Data only recoverable if column 3 & 4 are merged due to missing delimeter. i.e., professional_id and professional_id_anonymized.
    5. Use ETL configurations dictionary to check if we have valid inputs against 'event_type' column.
    6. Validate DateTime conversion for input TIMESTAMP. 
    7. Verify valid number of inputs in META_DATA column.
    8. Duplicate verifications within read chunk of data. (Saves overhead on Data Load)


    ### Assumptions:

    1. Date is in consistent format since it is from same source system. Complex formats can be handled separately, provision exists in the design.
    2. Reading Valid 'event_type' values from ETL configurations allow us to filter out invalid transactions using data validation function: fn_validate_and_update()
    3. A Professional will only be inserted in dimensions table upon "created_account" event. This way we can later identify through DB query to see which professionals have faulty dimensions data from source system.
"""


 
#Create and configure logger 
logging.basicConfig(filename="etl_debugs.log", 
                    format='[%(asctime)s - %(funcName)10s()]  %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                    filemode='w') 
  
#Creating an object 
logger=logging.getLogger() 
  
#Setting the threshold of logger to DEBUG 
logger.setLevel(logging.DEBUG) 

# ETL Configurations file path
etl_config_file = "__ETL_column_valids.config"

# "SOURCE" Data File path
events_data_file = "Dataset/event_log.csv"
# events_data_file = "Dataset/event_log-Copy.csv" # low size file for test

"""
    Connect to Database
"""
connection = psycopg2.connect(
    host="localhost",
    database="tz_test_posgres",
    user="postgres",
    password="postgres",
)
connection.autocommit = True


"""
    Load ETL configurations from file.
    IMP: Temporary basic logic added. More logic can be added for a complex configuration file. 
"""
ETL_configurations = {} # Global dictionary holding configurations
with(open(etl_config_file, encoding='utf8')) as f:
    logger.debug("Opened '__ETL_column_valids.config' file")
    
    try:
        ETL_conf = f.readline() # Expecting 'READ_CHUNK_SIZE'
        tmp_conf = ETL_conf.replace(' ', '').strip().split('=')
        logger.debug("Expecting 'READ_CHUNK_SIZE' from '__ETL_column_valids.config' file")
        if tmp_conf[0] == 'read_chunk_size': # Tag contains config for read_chunk_size
            logger.debug("Read 'read_chunk_size' configs from '__ETL_column_valids.config' file")
            ETL_configurations['read_chunk_size'] = tmp_conf[1]
            logger.debug('Successfully read read_chunk_size header from config file')
        else:
            logger.debug("ERROR while reading 'read_chunk_size'")
            raise ValueError

        ETL_conf = f.readline() # expecting 'EVENT_TYPE_DIM'
        tmp_conf = ETL_conf.replace(' ', '').strip().split('=')
        logger.debug("Expecting 'EVENT_TYPE_DIM' from '__ETL_column_valids.config' file")
        if tmp_conf[0] == 'event_type': # Tag contains config for 'EVENT_TYPE_DIM' dimension
            logger.debug("Read 'event_type' configs from '__ETL_column_valids.config' file")
            """
                Read possible event types from 'event_type' tag of config file and load the information to Global 
                    dictionary for reference later.
            """
            logger.debug('Trying to parse event_type from config file')
            tmp_e_split = [item.split(':') for item in tmp_conf[1].strip('][').split(',')]
            event_type_dim = dict([(i[0], int(i[1])) for i in tmp_e_split])
            # event_type_dim = dict([tuple(item.split(':')) for item in tmp_conf[1].strip('][').split(',')])
            
            ETL_configurations[tmp_conf[0]] = list(event_type_dim.keys())
            ETL_configurations['event_type_dim'] = event_type_dim

            pprint (ETL_configurations)
            logger.debug('Successfully read event_type header from config file')
        else:
            raise ValueError
        
        logger.debug("Loaded configuration file: ")
        logger.debug(ETL_configurations)
        
        logger.debug("Successfully loaded configurations from '__ETL_column_valids.config' file")
    except Exception as e:
        logger.debug("Please ensure that correct configuration file is at: {%s} location.", etl_config_file)
        logger.error("Exception occurred", exc_info=True)


"""
    *************************************************************
    In this section, I have included all helper functions used in below code to execute the functionalities
    **************************************************************
    
""" 

"""
    For all inserts, I used EXECUTE_VALUES() method to insert complete chunk into DB.
    
    Purpose: Boost insert performance.
    
    Assumption: Code is already eliminating duplicates within each chunk, so no duplicate data will pass through after.
    
    Possible Improvements: There could be duplicates intra-chunk; 
            A relatively slower approach has to be used to ignore duplicate inserts, or to handle differently. 
            (Used EXECUTE() method for that.)
            
    IMP: feed insert statement syntax to generated function for automomation.
"""

# Currently being only utilized for services_dim table due to nature of inserts
services_dim_syntax = 'insert into public.services_dim (%s) values %s'  
event_id_dim_syntax = 'insert into public.event_id_dim (%s) values %s' 

# Return insert statement for each row
def build_postgres_inserts(cursor, insert_statement, row): # pass isnert syntax too
    # This function builds insert queries from incoming chunk of data. Extracts KEYS from dictionary itself.
    columns = row.keys()
    values = row.values()
    return cursor.mogrify(insert_statement, (AsIs(','.join(columns)), tuple(values))) #can also remove columns initialization


def events_fact_chunk_to_DB(connection, chunk, page_size: int = 100, ) -> None:
    with connection.cursor() as cursor:
        logger.debug("Inside 'events_fact_chunk_to_DB' function. Attempting insert.")
        try:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO public.events_fact VALUES %s;
            """, ((
                row['event_id_key'],
                row['event_type_key'],
                row['professional_key'],
                row['date_key'],
                row['time_key']
            ) for row in chunk), page_size=page_size)  
        except psycopg2.Error: #        except psycopg2.IntegrityError:
            logger.error("ERROR: Primary key Index Violated. Current chunk has duplicate data.")
            logger.error("Failed in 'EVENTS_FACT_to_DB' --- Aborting chunk.")
            return False
        
    logger.debug("Data inserted successfully in 'events_fact' table. Returning status.")
    return True
    
def propose_events_fact_chunk_to_DB(connection, chunk, page_size: int = 100, ) -> None:
    logger.debug("Inside 'propose_events_fact_chunk_to_DB' function. Attempting insert.")
    with connection.cursor() as cursor:
        try:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO public.propose_events_fact VALUES %s;
            """, ((
                row['event_id_key'],
                row['event_type_key'],
                row['professional_key'],
                row['service_key'],
                row['date_key'],
                row['time_key'],
                row['lead_fee']
            ) for row in chunk), page_size=page_size)  
        except psycopg2.Error: #        except psycopg2.IntegrityError:
            print ('Duplicate value error')
            logger.error("ERROR: Primary key Index Violated. Current chunk has duplicate data.")
            logger.error("Failed in 'propose_events_fact_chunk_to_DB' --- Aborting chunk.")
            return False

    logger.debug("Data inserted successfully in 'propose_events_fact' table. Returning status.")
    return True


def event_id_dim_chunk_to_DB(connection, chunk, page_size: int = 100, ) -> None:
    with connection.cursor() as cursor:
        logger.debug("Inside 'event_id_dim_chunk_to_DB' function. Attempting insert.")
        print (chunk)
        try:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO public.event_id_dim VALUES %s;
            """, ((
                row['event_id_key']
            ) for row in chunk), page_size=page_size)  
        except psycopg2.Error: #        except psycopg2.IntegrityError:
            logger.error("ERROR: Primary key Index Violated. Current chunk has duplicate data.")
            logger.error("Failed in 'event_id_dim_chunk_to_DB' --- Aborting chunk.")
            return False
        
    logger.debug("Data inserted successfully in 'event_id_dim' table. Returning status.")
    return True

def professionals_dim_chunk_to_DB(connection, chunk, page_size: int = 100, ) -> None:
    with connection.cursor() as cursor:
        logger.debug("Inside 'professionals_dim_chunk_to_DB' function. Attempting insert.")
        try:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO public.professionals_dim VALUES %s;
            """, ((
                row['professional_key'],
                row['professional_id'],
                row['acc_creation_date']
            ) for row in chunk), page_size=page_size)  
        except psycopg2.Error: #        except psycopg2.IntegrityError:
            logger.error("ERROR: Primary key Index Violated. Current chunk has duplicate data.")
            logger.error("Failed in 'professionals_dim_chunk_to_DB' --- Aborting chunk.")
            return False
        
    logger.debug("Data inserted successfully in 'professionals_dim' table. Returning status.")
    return True

def insert_chunk_to_DB(connection, insert_syntax, chunk) -> None:
    """
        Requires: DB connection, INSERT QUERY Syntax, data chunk to insert
        
        Limitation: Row by Row insert - slower than execute_values()
    """
    with connection.cursor() as cursor:
        if chunk: # Check if chunk is NULL
            for row in chunk:
                try:
                    insert_query = build_postgres_inserts(cursor, insert_syntax, row)
                    cursor.execute(insert_query, row)
                    # print ('success for this trn')
                except psycopg2.Error as e:
                    logger.exception(e)
#                     print (e)
                connection.commit()
        return True

def get_data_chunk_list(reader, chunksize=100):
    """ RETURN TYPE: List of Dictionaries
    Chunk generator. Take a CSV `reader` and yield `chunksize` sized slices of data. 
    """
    chunk = []

    for index, line in enumerate(reader):
        if (index % chunksize == 0 and index > 0):
            yield chunk
            chunk.clear() # Clears entire dictionary
        chunk.append(dict(line))
    yield chunk


def search_in_list_dicts(inpList, key, value):
    """
        Small utility function to search into List of Dictionaries for a specified Key-Value pair.
    """
    for dct in inpList:
        if isinstance(dct, dict):
            try:
                if dct[key] == value:
                    return True
            except KeyError as e:
                continue
    return False

def validate_date(inpVal):
    """
        Assumption: Timestamp column in given events table is consistent due to being from one source system.
        
        Any complex date validations can be added in this function to process date
    """
    logger.debug('validating date format and conversion for input: {%s}', inpVal)
    
    try:
        inpVal = datetime.strptime(inpVal, '%Y-%m-%d %H:%M:%S')
        logger.debug('Validation successful; converted Timestamp: %s', inpVal)
        return inpVal
    except ValueError as e:
        logger.error('Input date is in invalid format, thus cannot be converted.')
        logger.error('validation failed; returning FALSE')
        return False
    
def fn_validate_and_update(inpEvent, srcDict):
    logger.debug('Inside function >fn_validate_and_update<')
    logger.debug('Attempting data-type conversion for event data')
    
    try:
        logger.debug('Going to convert input event_id: %s', inpEvent[0])
        inpEvent[0] = int(inpEvent[0])
        
        logger.debug('Going to convert input event_type: %s', inpEvent[1])
        inpEvent[1] = str(inpEvent[1])
        # Validate if incoming 'event_type' is valid or not.
        if inpEvent[1] not in ETL_configurations['event_type']:
            logger.error("ERROR:: 'Event_Type': {%s} of Event_id: {%s} is Invalid.", inpEvent[1], inpEvent[0])
            return False
        
        logger.debug('Going to convert input professional_id: %s', inpEvent[2])
        if inpEvent[2] == '':
            logger.debug('professional_id is null')
            inpEvent[2] = None
        else:
            inpEvent[2] = int(inpEvent[2])
        
        logger.debug('Going to convert input professional_id_anonymized: %s', inpEvent[3])
        if inpEvent[3] == '':
            logger.debug('professional_id_anonymized is null')
            inpEvent[3] = None
        else:
            inpEvent[3] = int(inpEvent[3])
        
        logger.debug('Going to convert input created_at %s', inpEvent[4])
        logger.debug("Calling 'validate_date' to process date")
        inpEvent[4] = validate_date(inpEvent[4])
        if inpEvent[4] == False: # Failed in timestamp conversion : reject transaction
            return False
        # Processing META_DATA column data
        logger.debug('Going to convert input meta_data %s', inpEvent[5])
        inpEvent[5] = str(inpEvent[5])
        logger.debug('conversion successful -- inpEvent[1]: {%s} ~~~ inpEvent[5]: {%s}', inpEvent[1], inpEvent[5]) 
        if (inpEvent[5] != '') and (inpEvent[1] == 'proposed'): # Any event that is not 'proposed', cannot have 'meta_data'
            logger.debug("'Propose' event : going to parse META_DATA column.")
            inpEvent[5] = inpEvent[5].split('_')
            if len(inpEvent[5]) != 4: # Ensure that meta_data tag has valid number of inputs
                logger.error("ERROR ('Column count is not 4') in meta_data of 'EVENT_ID': {%s} ---- %s", inpEvent[0], inpEvent[5])
                logger.error('META_DATA validation failed; returning FALSE')
                return False
        elif (inpEvent[5] != '') and (inpEvent[1] != 'proposed'): # Other events, Valid case
            logger.error("ERROR:: Only 'proposed' event can have valid meta_data input.")
            logger.error("Event data of 'event_id': {%s} is in invalid format, thus cannot be converted.", inpEvent[0])
            return False 
        srcDict['event_id'], srcDict['event_type'], srcDict['professional_id'], srcDict['pid_anonymized'], \
            srcDict['created_at'], srcDict['meta_data'] = inpEvent[0], inpEvent[1], inpEvent[2], inpEvent[3], inpEvent[4], inpEvent[5]
        
        logger.debug("'fn_validate_and_update' successful; returning now.")
        return True
    except Exception as e:
        logger.error('Event data is in invalid format, thus cannot be converted.')
        logger.error('validation failed; returning FALSE')
        return False
        

def build_event_dictionaries(inpEvent):
    """Append in the main CHUNK lists"""
#     event_id_dim, event_type_dim, professionals_dim, services_dim = [], [], [], []
#     events_fact, propose_events_fact = [], []

    logger.debug("Inside 'build_event_dictionaries' function")
    
    event_id_dict, event_type_dict, professional_dict, event_fact_dict, service_dict, propose_fact_dict = {}, {}, {}, {}, {}, {}
    """
        Two events with duplicate Event-ID are to be filtered. Only one out of both will get inserted.
        Duplicate event will be sent to error log and transaction will be aborted.
    """
    #event_id_dimension
    event_id_dict['event_id_key'] = inpEvent['event_id']
    
    logger.debug("Checking for encounter of 'event_id_key': {%s} in current chunk", inpEvent['event_id'])
    if search_in_list_dicts(event_id_dim, 'event_id_key', inpEvent['event_id']) == False:
        event_id_dim.append(event_id_dict) # Adding dimension record to event_id_dim list
        logger.debug("Added dimension row to event_id_dim list ---- 'event_id_key': {%s}", inpEvent['event_id'])
    else:
        # Duplicate record within read chunk : rejct event
        logger.debug("Record already exists in event_id_dim list ---- 'event_id_key': {%s}", inpEvent['event_id'])
        logger.debug('Insertion not required - Aborting transaction')
        return False # Break transaction on duplicate
    
    #event_type_dimension (STATIC Pre-populated TABLE - Handle outside ETL : Using ETL_CONFIGURATIONS file)
    # Only use it to populate row for FACT table
    event_type_dict['event_type_key'] = inpEvent['event_type'] # Fetch event_type_key (numeric value)
    event_type_key = ETL_configurations['event_type_dim'][inpEvent['event_type']]
    
    #srcDict['event_type'], srcDict['professional_id'], srcDict['pid_anonymized'], \
#             srcDict['created_at'], srcDict['meta_data']
    
    #professional_dimension (id, pid, pid_anonym, join_date, )
    if inpEvent['event_type'] == 'created_account': # populate "account creation date" for "created_account" event
        logger.info("New account creation for professional_key: {%s}", inpEvent['pid_anonymized'])
        
        # professionals_dimension will only be populated for account creation event
        professional_dict['professional_key'] = inpEvent['pid_anonymized']
        professional_dict['professional_id'] = inpEvent['professional_id']
        professional_dict['acc_creation_date'] = inpEvent['created_at']
        
        if search_in_list_dicts(professionals_dim, 'professional_key', inpEvent['pid_anonymized']) == False:
            professionals_dim.append(professional_dict) # Adding dimension record to event_id_dim list
            logger.debug("Added dimension row to professionals_dim list ---- 'pid_anonymized': {%s}", inpEvent['pid_anonymized'])
        else:
            # Duplicate record within read chunk
            logger.debug("Record already exists in professionals_dim list ---- 'pid_anonymized': {%s}", inpEvent['pid_anonymized'])
            logger.debug('Insertion not required')
            # Breaking of transaction not required on duplicate
        logger.debug("Added row to 'professional_dict' for professional_key: {%s}", inpEvent['pid_anonymized'])
    
    
    if inpEvent['event_type'] != 'proposed':
        logger.debug("event_id_key: {%s} is NOT a 'proposed' event.", inpEvent['event_id'])
        logger.debug("Proceeding to build EVENT_FACT_DICT for event logging")
        
        if inpEvent['meta_data']: # Any event that is not 'proposed', cannot have 'meta_data'
            logger.error("Any event beside 'proposed', cannot have 'meta_data' value.")
            logger.error("Aborting 'EVENT_FACT_DICT' insertion for event_id_key: {%s}", inpEvent['event_id'])
            return False
        
        try:
            # Event fact dictionary populate - event_fact_dict
            event_fact_dict['event_id_key'] = inpEvent['event_id']
            event_fact_dict['event_type_key'] = event_type_key
            event_fact_dict['professional_key'] = inpEvent['pid_anonymized']  #--------(pid_anonymized is surrogate key)
            event_fact_dict['date_key'] = int(inpEvent['created_at'].strftime('%Y%m%d')) # make key as per DATE_KEY logic
            event_fact_dict['time_key'] = int(inpEvent['created_at'].strftime('%H%M')) # make key as per TIME_KEY logic
            
            logger.debug("Event_fact_dict: %s", event_fact_dict)
            events_fact.append(event_fact_dict) # Adding event record
            logger.debug("Added row to 'event_fact_dict' for event_id_key: {%s}", inpEvent['event_id'])
        except Exception as e:
            logger.error("Error Occured: {%s}", e)
            logger.error("Aborting 'EVENT_FACT_DICT' insertion for event_id_key: {%s}", inpEvent['event_id'])
            return False #Operation failed
        
    
    else: # elif inpEvent['event_type'] == 'proposed': ---- In case some future behavior segregation comes
        # process and build SERVICES_DIMENSION and PROPOSE_EVENT_FACT dictionaries for 'proposed' event
        logger.debug("event_id_key: {%s} is a 'proposed' event.", inpEvent['event_id'])
        logger.debug("Going to build SERVICES_DIMENSION and PROPOSE_EVENT_FACT dictionaries")
        
        # Service_dimension (service_id, service_name_nl, service_name_en)
        """
            Best way to handle SERVICES Static dimension table is through a separate ETL of its own, 
                just like EVENT_TYPE_DIM. Here it is an overhead but I will populate it by first grouping services
                to each data CHUNK level and then test against DB in main cursor if record does not exist for incoming
                Service.
        """
        meta_data = inpEvent['meta_data']
        logger.debug("Checking for encounter of 'service_key': {%s} in current chunk", meta_data[0])

        if search_in_list_dicts(services_dim, 'service_key', meta_data[0]) == False:
            logger.debug("First time encountering 'service_key': {%s} --- 'service_name_nl': {%s} in current chunk", meta_data[0], meta_data[1])
            service_dict['service_key'] = meta_data[0]
            service_dict['service_name_nl'] = meta_data[1]
            service_dict['service_name_en'] = meta_data[2]
            services_dim.append(service_dict) # Adding dimension record to services_dim list
            logger.debug("Added dimension row to services_dim list ---- 'service_key': {%s}", meta_data[0])
        else:
            logger.debug("Record already exists in services_dim list ---- 'service_key': {%s}", meta_data[0])
            logger.debug('Insertion not required')
        
        logger.debug("Proceeding to build PROPOSE_FACT_DICT for event logging")
        
        try:
            # Propose Event fact dictionary populate - propose_fact_dict
            propose_fact_dict['event_id_key'] = inpEvent['event_id']
            propose_fact_dict['event_type_key'] = event_type_key
            propose_fact_dict['professional_key'] = inpEvent['pid_anonymized']  #--------(pid_anonymized is surrogate key)
            propose_fact_dict['service_key'] = meta_data[0] # Ref main variable as service_dict will be updated conditionally 
            propose_fact_dict['date_key'] = int(inpEvent['created_at'].strftime('%Y%m%d')) # make key as per DATE_KEY logic
            propose_fact_dict['time_key'] = int(inpEvent['created_at'].strftime('%H%M')) # make key as per TIME_KEY logic
            propose_fact_dict['lead_fee'] = meta_data[3]

            logger.debug("Propose_fact_dict: %s", propose_fact_dict)
            propose_events_fact.append(propose_fact_dict)
            logger.debug("Added row to 'propose_events_fact' for event_id_key: {%s}", inpEvent['event_id'])
        except Exception as e:
            logger.error("Error Occured: {%s}", e)
            logger.error("Aborting 'PROPOSE_FACT_DICT' insertion for event_id_key: {%s}", inpEvent['event_id'])
            return False #Operation failed
        
    logger.debug("'build_event_dictionaries' successfully completed")
#     logger.debug("*************************************************")
    logger.debug("*************************************************************")
    logger.debug("******************** [[Event Logged]] ***********************")
    logger.debug("*************************************************************")
#     logger.debug("*************************************************")
    return True

"""
    ## Functions responsible for building data dictionaries for Postgres Cursors

    Below series of functions will work to create dictionaries from incoming data chunks to then push to Database of choice, i.e., Postgres
"""

def parse_chunk_create_dictionary (inpChunk):
    # Initialize the Iterator object for handing over Chunk data to DB cursors
    event_id_dim, event_type_dim, professionals_dim, services_dim = [], [], [], []
    events_fact, propose_events_fact = [], []
    
    for trn in inpChunk:
        # we have each TRANSACTION HERE
        """Expand the transaction into in-memory variables
                NOTE: Convert to appropriate datatypes in TRY-Expcpt"""
        trn_data_list = [] # List created to hold the data to be Validated and Updated
        
        try:
            event_id, event_type, professional_id, pid_anonymized, created_at, meta_data \
                    = trn['event_id'], trn['event_type'], trn['professional_id'], trn['pid_anonymized'], trn['created_at'], trn['meta_data']
            
            logger.debug('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            logger.debug('Event row for Event_ID: {%s} is being processed.', event_id)
            
            trn_values_List = list(trn.values())
            trn_count_nones = trn_values_List.count(None) # Count = 1, we try to recover. Count = 2, we ignore transaction.
            
            logger.debug('Running validity tests on event with Event_ID: {%s}.', event_id)
            
            if trn_count_nones <= 1: # Check if there are invalid columns
                """check what nature of EVENT it is"""
                if event_type in ETL_configurations['event_type']: # Event_type is valid
                    if trn_count_nones == 0:
                        # Perform checks to see if column values are of valid data types
                        # print ('Valid event') # Move transaction for process
                        
                        # Process the transaction 
                        trn_data_list = [event_id, event_type, professional_id, pid_anonymized, created_at, meta_data]
                        # print (trn_data_list)
                        logger.debug ("calling 'fn_validate_and_update' for event_id: {%s}", event_id)
                            
                        if fn_validate_and_update(trn_data_list, trn): # in-place modification
                            # send complete data for building of our dictionaries
                            build_event_dictionaries(trn) # handles both types of events automatically  
                            
                    else: # trn_count_nones = 1 (1 null value)
                        if len(professional_id) < 7: # Column 'professional_id' is NULL and replaced by 'pid_anonymized'
                            logger.debug('professional_id column is null; swapping values')
                            meta_data, created_at = created_at, pid_anonymized
                            pid_anonymized, professional_id = professional_id, ''
                            logger.debug('swapping completed')
                            
                            # Process the transaction 
                            trn_data_list = [event_id, event_type, professional_id, pid_anonymized, created_at, meta_data]
                            # print (trn_data_list)
                            logger.debug ("calling 'fn_validate_and_update' for event_id: {%s}", event_id)
                            
                            if fn_validate_and_update(trn_data_list, trn): # in-place modification
                                # send complete data for building of our dictionaries
                                build_event_dictionaries(trn) # handles both types of events automatically  
                            
                            # if trn_data_list[5]: # event is 'PROPOSE' : Additional data available
                              #  # send data to build PROPOSE_EVENT & SERVICE_DIMENSION dicts.
                               # pass
                            
                        elif (len(professional_id) > 7) & (professional_id.isalnum()): # Case handling: Column 3 & 4 are merged
                            # these are separable
                            logger.debug('professional_id has pid_anonymized column merged; attempting fix.')
                            logger.debug('professional_id: {%s} is merged.', professional_id)
#                             print (professional_id)
                            meta_data, created_at = created_at, pid_anonymized
                            pid_anonymized, professional_id = professional_id[7:], professional_id[:7] 
                            logger.debug('After Fix => -- professional_id: {%s}, -- pid_anonymized: {%s}.', professional_id, pid_anonymized)
                            print ('found it hereeee: {} --- {}'.format(pid_anonymized, professional_id))
                            
                            # Process the transaction 
                            trn_data_list = [event_id, event_type, professional_id, pid_anonymized, created_at, meta_data]
                            # print (trn_data_list)
                            
                            if fn_validate_and_update(trn_data_list, trn): # in-place modification
                                # send complete data for building of our dictionaries
                                build_event_dictionaries(trn) # handles both types of events automatically  
                            
                        else:
                            # Case not handled in ETL : reject the transaction
                            pass

                else:
                    # EVENT_TYPE is invalid : reject the transaction
                    logger.debug('Event type is invalid; not matching config file.')
                    logger.debug('Event row for Event_ID: {%s} is rejected.', event_id)
                    print ('EVENT_TYPE is invalid: {}'.format(trn))
            elif trn_count_nones > 1:
                # More than 1 invalid column : reject the transaction
                logger.debug('Invalid column count is greater than 1.')
                logger.debug('Event row for Event_ID: {%s} is rejected.', event_id)
#                 print ('trn_NONES > 2: {}'.format(trn_count_nones))
        except Exception as e:
            logger.error("Exception occurred while parsing event row", exc_info=True)
            logger.error(trn)
            logger.debug('Moving to process next event')
            continue    
            
            
            
            

i, chunks_required = 0, 1
input_data = {}
chunkyy = ''
logging.shutdown()

event_id_dim, event_type_dim, professionals_dim, services_dim = [], [], [], []
events_fact, propose_events_fact = [], []

def persist_chunk_to_db (chunk_number):
    """
        Makes use of global variables to persist data to DB
    """
    if events_fact_chunk_to_DB(connection, events_fact, 10):
        print ("'EVENTS_FACT' data inserted successfully for Chunk # {%s}", chunk_number)
    else:
        print ('error occured')    
        
    if propose_events_fact_chunk_to_DB(connection, propose_events_fact, 10):    
        print ("'PROPOSE_EVENTS_FACT' data inserted successfully for Chunk # {%s}", chunk_number)
    else:
        print ('error occured')

        # if event_id_dim_chunk_to_DB(connection, event_id_dim, 10): # Facing some INT conversion issue in bulk upload due to no other columns
    if insert_chunk_to_DB(connection, event_id_dim_syntax, event_id_dim):
        logger.debug ("'event_id_dim' data inserted successfully for Chunk # {%s}", chunk_number)
    else:
        print ('error occured in event_id_dim')
        
    if professionals_dim_chunk_to_DB(connection, professionals_dim, 10):    
        print ("'professionals_dim' data inserted successfully for Chunk # {%s}", chunk_number)
    else:
        print ('error occured')
        
    if insert_chunk_to_DB(connection, services_dim_syntax, services_dim):    
        print ("'services_dim' data inserted successfully for Chunk # {%s}", chunk_number)
    else:
        print ('error occured in services_dim')
            

field_names = ['event_id', 'event_type', 'professional_id', 'pid_anonymized', 'created_at', 'meta_data']
with(open(events_data_file, encoding='utf8')) as f:
    f.seek(0) # Go to start
    next(f) # Skip header
    csvDictRead = csv.DictReader(f, delimiter=',', fieldnames=field_names)
    for chunk in get_data_chunk_list(csvDictRead, int(ETL_configurations['read_chunk_size'])):
        # Reset chunk dictionaries for fresh data
        event_id_dim, event_type_dim, professionals_dim, services_dim = [], [], [], []
        events_fact, propose_events_fact = [], []

        parse_chunk_create_dictionary(chunk)
        
        if chunks_required == 5: # Execution limiter removed for full execution
            break
        persist_chunk_to_db (chunks_required)

        
        chunks_required += 1
        
        print ('\n')
        print ('completed insert')


# print ('completed')
# print (input_data)

logging.shutdown()