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
            
    if isinstance(field['type'], list):
        if field['type'][0] == "null":
            schema_mode = 'NULLABLE'
        else:
            schema_mode = 'required'
        schema_type = field['type'][-1]
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
        t_type = field.get('items').get('type')
        if isinstance(t_type, list):
            schema_type = t_type[-1]
        else:
            schema_type = t_type

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

def build_schema(schema):
    SCHEMA = []
    logger.info("start building schema")
    for key in schema['properties'].keys():
        logger.info("building schema with key {}".format(key))
        if not (bool(schema['properties'][key])):
            # if we endup with an empty record.
            continue

        schema_name, schema_type, schema_mode, schema_description, schema_fields = define_schema(schema['properties'][key], key)
        logger.info("building schema with %s %s %s %s %s", schema_name, schema_type, schema_mode, schema_description, schema_fields)
        SCHEMA.append(SchemaField(schema_name, schema_type, schema_mode, schema_description, schema_fields))

    return SCHEMA

def formatRecord(json):
    for key in json.keys():
        new_key = formatName(key, "field")
        json[new_key] = json.pop(key)
        if isinstance(json[new_key], dict) == True:
            json[new_key] = formatRecord(json[new_key])
    return json

def persist_lines_job(project_id, dataset_id, credentials=None, lines=None, truncate=False, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    bigquery_client = bigquery.Client(project=project_id,credentials=credentials)

    # try:
    #     dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    # except exceptions.Conflict:
    #     pass

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

def persist_lines_stream(project_id, dataset_id, credentials=None, lines=None, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    bigquery_client = bigquery.Client(project=project_id,credentials=credentials)

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

            if validate_records:
                validate(msg.record, schema)

            j = simplejson.dumps(msg.record)
            jparsed = simplejson.loads(j, use_decimal=False)
            jparsedFormated = formatRecord(jparsed)
            
            # logger.info("Streaming for {}".format(jparsedFormated))

            err = bigquery_client.insert_rows_json(tables[msg.stream], [jparsedFormated], ignore_unknown_values=True, skip_invalid_rows=False)
            if len(err):
                logger.error("Error syncing object {} with formatted payload {} got the following errors {}".format(msg.stream, jparsedFormated, err))
            errors[msg.stream] = err
            rows[msg.stream] += 1

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream 
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            logger.info("Dealing with {}".format(table))
            logger.info("Table Schema Info - {}".format(dataset.table(table)))
            logger.info("Raw Schema Info - {}".format(schemas[table]))
            logger.info("Schema Info - {}".format(build_schema(schemas[table])))
            tables[table] = bigquery.Table(dataset.table(formatName(table, "table")), schema=build_schema(schemas[table]))
            rows[table] = 0
            errors[table] = None
            try:
                tables[table] = bigquery_client.create_table(tables[table])
            except exceptions.Conflict:
                pass

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in errors.keys():
        if not errors[table]:
            logging.info('Loaded {} row(s) into {}:{}'.format(rows[table], dataset_id, table, tables[table].path))
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

    if not args.config.get('disable_collection', False):
        logger.info('Sending version information to stitchdata.com. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=collect).start()

    if args.config.get('replication_method') == 'FULL_TABLE':
        truncate = True
    else:
        truncate = False

    validate_records = args.config.get('validate_records', True)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    if args.config.get('client_secrets'):
        credentials = service_account.Credentials.from_service_account_info(args.config['client_secrets'], scopes=SCOPES)
    else:
        credentials = None

    if args.config.get('stream_data', True):
        state = persist_lines_stream(args.config['project_id'], args.config['dataset_id'], credentials, input, validate_records=validate_records)
    else:
        state = persist_lines_job(args.config['project_id'], args.config['dataset_id'], credentials, input, truncate=truncate, validate_records=validate_records)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
