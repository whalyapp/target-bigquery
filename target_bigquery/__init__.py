#!/usr/bin/env python3

import argparse
import io
import sys
import json
import simplejson
import logging
import collections
import threading
import http.client
import urllib
import calendar
import time
import pkg_resources
from pathlib import Path
import re
import random

from jsonschema import validate
import singer
from singer import utils

from tempfile import TemporaryFile

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import LoadJobConfig
from google.api_core import exceptions

from target_bigquery.stream_tracker import BufferedSingerStream

REQUIRED_CONFIG_KEYS = [
    "project_id",
    "dataset_id",
    "validate_records"
]

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = ['https://www.googleapis.com/auth/bigquery','https://www.googleapis.com/auth/bigquery.insertdata']
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Singer BigQuery Target'

StreamMeta = collections.namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

SINGER_RECEIVED_AT = '_wly_received_at'
SINGER_BATCHED_AT = '_wly_batched_at'
SINGER_SEQUENCE = '_wly_sequence'
SINGER_TABLE_VERSION = '_wly_table_version'
SINGER_PK = '_wly_primary_key'
SINGER_SOURCE_PK_PREFIX = '_wly_source_key_'
SINGER_LEVEL = '_wly_level_{}_id'
SINGER_VALUE = '_wly_value'

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def clear_dict_hook(items):
    return {k: v if v is not None else '' for k, v in items}

def formatName(name, recordType="table"):
    formattedName = re.sub('[^A-Za-z0-9_]', "_", name)
    if (formattedName[0].isdigit()):
        formattedName = "_" + formattedName
        
    if (recordType == "table"):
        # if (formattedName == "Table" or formattedName == "table"):
        #     formattedName = formattedName + "-" + "1"
        return formattedName[:1024]
    else:
        return formattedName[:128]

def define_schema(field, name):
    schema_name = formatName(name, "field")
    schema_type = "STRING"
    schema_mode = "NULLABLE"
    schema_description = None
    schema_fields = ()

    if 'type' not in field and 'anyOf' in field:
        for types in field['anyOf']:
            if types['type'] == 'null':
                schema_mode = 'NULLABLE'
            else:
                field = types

    #logger.info('defining schema for %s', field)
    if isinstance(field['type'], list):
        if ("null" in field['type']):
            schema_mode = 'NULLABLE'
        else:
            schema_mode = 'required'
        schema_type = field['type'][-1] if field['type'][0] == "null" else field['type'][0]
        logger.debug('found schema type in list {}'.format(schema_type))
    else:
        schema_type = field['type']

    if schema_type == "object":
        schema_type = "RECORD"
        logger.debug("builing for object {}".format(schema_name))
        schema_fields = tuple(build_schema(field))
        logger.debug("built for object {}".format(schema_name))
        logger.debug("with {}".format(schema_fields))
    if schema_type == "array":

        if 'anyOf' in field.get('items') and len(field.get('items').get('anyOf')) > 0:
            t_type = field.get('items').get('anyOf')[0].get('type')
        else:
            t_type = field.get('items').get('type')
        #logger.info('t_type: %s', t_type)

        if isinstance(t_type, list):
            schema_type = t_type[-1] if t_type[0] == "null" else t_type[0]
        else:
            schema_type = t_type
        #logger.info('schema_type: %s', schema_type)

        if schema_type == "array":
            return define_schema(field.get('items'), name)
            
        schema_mode = "REPEATED"
        if schema_type == "object":
            schema_type = "RECORD"
            logger.debug("builing for object in array {}".format(schema_name))
            schema_fields = tuple(build_schema(field.get('items')))
            logger.debug("built for object in array {}".format(schema_name))
            logger.info("with {}".format(schema_fields))


    if schema_type == "string":
        if "format" in field:
            if field['format'] == "date-time":
                schema_type = "timestamp"

    if schema_type == 'number':
        schema_type = 'FLOAT'

    return (schema_name, schema_type, schema_mode, schema_description, schema_fields)

def build_schema(schema, initial=False):
    SCHEMA = []
    logger.info("start building schema")

    properties = schema['properties']

    if initial == True:
        if SINGER_RECEIVED_AT not in properties:
            properties[SINGER_RECEIVED_AT] = {
                'type': ['null', 'string'],
                'format': 'date-time'
            }

        if SINGER_SEQUENCE not in properties:
            properties[SINGER_SEQUENCE] = {
                'type': ['null', 'integer']
            }

        if SINGER_TABLE_VERSION not in properties:
            properties[SINGER_TABLE_VERSION] = {
                'type': ['null', 'integer']
            }

        if SINGER_BATCHED_AT not in properties:
            properties[SINGER_BATCHED_AT] = {
                'type': ['null', 'string'],
                'format': 'date-time'
            }

        if SINGER_PK not in properties:
            properties[SINGER_PK] = {
                'type': ['null', 'string']
            }

    # logger.info("GENERATED SCHEMA - {}".format(properties))

    for key in properties.keys():
        logger.info("building schema with key {}".format(key))
        if not (bool(properties[key])):
            # if we endup with an empty record.
            continue

        schema_name, schema_type, schema_mode, schema_description, schema_fields = define_schema(properties[key], key)
        #logger.info("building schema with %s %s %s %s %s", schema_name, schema_type, schema_mode, schema_description, schema_fields)
        schema_field = SchemaField(schema_name, schema_type, schema_mode, schema_description, schema_fields)
        #logger.info("schema_field: {}".format(schema_field))
        SCHEMA.append(schema_field)

    #logger.info("returning schema %s", SCHEMA)
    return SCHEMA

def formatRecord(json):
    for key in json.keys():
        new_key = formatName(key, "field")
        json[new_key] = json.pop(key)
        if isinstance(json[new_key], dict) == True:
            json[new_key] = formatRecord(json[new_key])
    return json

def buildPrimaryKey(record, key_properties):
    primaryKey = ""
    for key in key_properties:
        formattedKey = formatName(key, "field")
        primaryKey = str(record[formattedKey]) if primaryKey == "" else primaryKey + ":" + str(record[formattedKey])
    return primaryKey

def persist_lines_job(project_id, dataset_id, credentials=None, lines=None, truncate=False, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    bigquery_client = bigquery.Client(project=project_id,credentials=credentials)

    try:
        dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    except exceptions.Conflict:
        pass

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            # we should apply the same transformation to the object key than the table / field names...
            record = msg.record
            logger.info("processing record in job - {}".format(record))
            

            # NEWLINE_DELIMITED_JSON expects literal JSON formatted data, with a newline character splitting each row.
            dat = bytes(json.dumps(record) + '\n', 'UTF-8')

            rows[msg.stream].write(dat)
            #rows[msg.stream].write(bytes(str(msg.record) + '\n', 'UTF-8'))

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream 
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            #tables[table] = bigquery.Table(dataset.table(table), schema=build_schema(schemas[table]))
            rows[table] = TemporaryFile(mode='w+b')
            errors[table] = None
            # try:
            #     tables[table] = bigquery_client.create_table(tables[table])
            # except exceptions.Conflict:
            #     pass

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in rows.keys():
        table_ref = bigquery_client.dataset(dataset_id).table(table)
        SCHEMA = build_schema(schemas[table])
        load_config = LoadJobConfig()
        load_config.schema = SCHEMA
        load_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON

        if truncate:
            load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

        rows[table].seek(0)
        logger.info("loading {} to Bigquery.\n".format(table))
        load_job = bigquery_client.load_table_from_file(
            rows[table], table_ref, job_config=load_config)
        logger.info("loading job {}".format(load_job.job_id))
        logger.info(load_job.result())


    # for table in errors.keys():
    #     if not errors[table]:
    #         print('Loaded {} row(s) into {}:{}'.format(rows[table], dataset_id, table), tables[table].path)
    #     else:
    #         print('Errors:', errors[table], sep=" ")

    return state

def persist_lines_stream(project_id, dataset_id, credentials=None, lines=None, validate_records=True, collision_suffix=None, current_batch=None):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    bigquery_client = bigquery.Client(project=project_id,credentials=credentials)
    buffered_singer_stream = BufferedSingerStream(bigquery_client)

    dataset_ref = bigquery_client.dataset(dataset_id)
    dataset = Dataset(dataset_ref)
    try:
        dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    except exceptions.Conflict:
        pass

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]
            current_time = calendar.timegm(time.gmtime())

            if validate_records:
                try:
                    validate(msg.record, schema)
                except:
                    logger.error("unable to validate {}".format(msg.record))

            # logger.info("Got the following schema - {}".format(schema))
            # logger.info("got the following metadata - {}".format(key_properties[msg.stream]))

            j = simplejson.dumps(msg.record)
            jparsed = simplejson.loads(j, use_decimal=False)
            jparsedFormated = formatRecord(jparsed)

            if msg.version:
                jparsedFormated[SINGER_TABLE_VERSION] = str(msg.version)

            if msg.time_extracted and jparsedFormated.get(SINGER_RECEIVED_AT) is None:
                jparsedFormated[SINGER_RECEIVED_AT] = str(msg.time_extracted)


            # if self.use_uuid_pk and record.get(singer.PK) is None:
            jparsedFormated[SINGER_PK] = buildPrimaryKey(jparsedFormated, key_properties[msg.stream])
            # logger.info("got the following primary key - {}".format(jparsedFormated[SINGER_PK]))

            jparsedFormated[SINGER_BATCHED_AT] = current_batch

            jparsedFormated[SINGER_SEQUENCE] = current_time
                
            # logger.info("Streaming for {}".format(jparsedFormated))
            buffered_singer_stream.add_record_message(tables[msg.stream], jparsedFormated)
            # err = bigquery_client.insert_rows_json(tables[msg.stream], [jparsedFormated], ignore_unknown_values=True, skip_invalid_rows=False)
            # if len(err):
            #     logger.error("Error syncing object {} with formatted payload {} got the following errors {}".format(msg.stream, jparsedFormated, err))
            # errors[msg.stream] = err
            rows[msg.stream] += 1

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream

            # Some schema are coming without pre`operties, just ignore them
            if not 'properties' in msg.schema:
                continue

            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            logger.info("Dealing with {}".format(table))
            logger.info("Table Schema Info - {}".format(dataset.table(table)))
            logger.info("Raw Schema Info - {}".format(schemas[table]))
            logger.info("Schema Info - {}".format(build_schema(schemas[table])))
            tables[table] = bigquery.Table(dataset.table(formatName(table, "table")), schema=build_schema(schemas[table], initial=True))
            rows[table] = 0
            errors[table] = None
            try:
                logger.info("creating table {}".format(tables[table]))
                tables[table] = bigquery_client.create_table(tables[table])
            except exceptions.Conflict:
                pass

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))
    
    buffered_singer_stream.flush_buffer()

    for table in errors.keys():
        if not errors[table]:
            logging.info("Loaded {} row(s) into {}:{}".format(rows[table], dataset_id, table, tables[table].path))
            tableName = "{}.{}".format(dataset_id, table)
            sql = """
MERGE {} t
USING (
  SELECT row[OFFSET(0)].* FROM (
    SELECT ARRAY_AGG(t ORDER BY t._wly_batched_at DESC LIMIT 1) row
    FROM {} t
    GROUP BY t._wly_primary_key 
  ) 
) s
ON t._wly_primary_key = s._wly_primary_key and t._wly_batched_at = s._wly_batched_at
WHEN NOT MATCHED BY SOURCE THEN DELETE
WHEN NOT MATCHED BY TARGET THEN INSERT ROW
            """.format(tableName,tableName)
            logging.info('Executing the final transformation into {}'.format(tableName))
            try:
                query_job = bigquery_client.query(sql)
                results = query_job.result()
                if results:
                    logger.info("Cleaning table {} with sql query {} got the following results {}".format(tableName, sql, results))
            except:
                logger.error("Failed Cleaning the Table {}".format(tableName))

            emit_state(state)
        else:
            logging.error('Errors: {}'.format(errors[table]))

    return state

def collect():
    try:
        version = pkg_resources.get_distribution('target-bigquery').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-bigquery',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')

def load_json(path):
    with open(path) as f:
        return json.load(f)

def process_args():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If using a service account, validate that the client_secrets.json file exists and load it
    if args.config.get('key_file_location'):
        if Path(args.config['key_file_location']).is_file():
            try:
                args.config['client_secrets'] = load_json(args.config['key_file_location'])
            except ValueError:
                logger.critical("tap-google-analytics: The JSON definition in '{}' has errors".format(args.config['key_file_location']))
                sys.exit(1)
        else:
            logger.critical("tap-google-analytics: '{}' file not found".format(args.config['key_file_location']))
            sys.exit(1)

    return args

def main():
    args = process_args()
    current_batch = calendar.timegm(time.gmtime())

    if not args.config.get('disable_collection', False):
        logger.info('Sending version information to stitchdata.com. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=collect).start()

    if args.config.get('replication_method') == 'FULL_TABLE':
        truncate = True
    else:
        truncate = False

    if args.config.get('collision_suffix'):
        collision_suffix = args.config.get('collision_suffix')

    validate_records = args.config.get('validate_records', True)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    if args.config.get('client_secrets'):
        credentials = service_account.Credentials.from_service_account_info(args.config['client_secrets'], scopes=SCOPES)
    else:
        credentials = None

    if args.config.get('stream_data', True):
        state = persist_lines_stream(args.config['project_id'], args.config['dataset_id'], credentials, input, validate_records=validate_records, current_batch=current_batch)
    else:
        state = persist_lines_job(args.config['project_id'], args.config['dataset_id'], credentials, input, truncate=truncate, validate_records=validate_records)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
