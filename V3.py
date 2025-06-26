import zipfile
import gzip
import json
import uuid
import logging
import pandas as pd
import csv
import os
import sys
from datetime import datetime

all_data = []
output_file_path = ''
entity_list = []

# Set up logging
def setup_logging():
    global logger
    logger = logging.getLogger("EntityExtractUtility")
    logger.setLevel(logging.DEBUG)
    log_file_path = os.path.join(config_file["OutputLocation"], f"Output_Logs_{timestamp_unix_ms}.log")
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Loads JSON data from a file and appends it to a global list all_data
def load_json(file):
    try:
        data = json.load(file)
        all_data.extend(data)
    except Exception as e:
        logger.error(f"An error occurred while loading JSON data: {e}")
    return all_data

# Changes the header of a DataFrame to the values in the InputLocationMappingFile if HeaderFormat is set to "Label"
def change_dataframe_headers(df, dict_map):
    if config_file["HeaderFormat"] == "Label":
        header_mapping = {key: value if value else key for key, value in dict_map["Attributes"].items()}
        df.rename(columns=header_mapping, inplace=True)
    return df

# Converts nested child JSON records to a CSV file and writes them to a specified location
def nested_child_json_to_csv(k, key, nested_child_list, entity_name):
    try:
        logger.info(f"Nested Child records transformation from JSON to CSV for {k} {key}")
        entity_nested_child_df = pd.DataFrame(nested_child_list)
        entity_nested_child_df = change_dataframe_headers(entity_nested_child_df, dict_map)
        nested_child_name = f"{entity_name}_{k}_{key}_Records_{timestamp_unix_ms}"
        csv_file_path_child = os.path.join(config_file["OutputLocation"], f"{nested_child_name}.csv")
        
        if os.path.isfile(csv_file_path_child):
            entity_nested_child_df.to_csv(csv_file_path_child, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, mode='a', header=False, index=None, na_rep='')
        else:
            entity_nested_child_df.to_csv(csv_file_path_child, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, index=None, na_rep='')
        logger.info(f"Nested child CSV file created: {csv_file_path_child}")
    except Exception as e:
        logger.error(f"An error occurred while transforming nested child records from JSON to CSV: {e}")

# Converts child JSON records to a CSV file and writes them to a specified location
def child_json_to_csv(k, child_list, entity_name):
    try:
        logger.info(f"Child records transformation from JSON to CSV for {k}")
        entity_child_df = pd.DataFrame(child_list)
        entity_child_df = change_dataframe_headers(entity_child_df, dict_map)
        child_name = f"{entity_name}_{k}_Records_{timestamp_unix_ms}"
        csv_file_path_child = os.path.join(config_file["OutputLocation"], f"{child_name}.csv")
        
        if os.path.isfile(csv_file_path_child):
            entity_child_df.to_csv(csv_file_path_child, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, mode='a', header=False, index=None, na_rep='')
        else:
            entity_child_df.to_csv(csv_file_path_child, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, index=None, na_rep='')
        logger.info(f"Child CSV file created: {csv_file_path_child}")
    except Exception as e:
        logger.error(f"An error occurred while transforming child records from JSON to CSV: {e}")

# Processes entity information from JSON data, handling attributes and nested structures, and appends the processed data to entity_list.
def getEntityInfo(entity_list, i, dict_map, record_dict, entity_name):    
    child_list = []
    dict_child_struct = {}
    nested_child_dict = {}
    nested_child_list = []
    try:
        for k1, v1 in i.items():
            if k1 == 'attributes':
                if str(v1) != "{}":
                    for k, v in i[k1].items():
                        try:
                            if k in dict_map["Attributes"]:
                                if config_file["SimpleAttributePublishType"] == "MultipleOVs":
                                    values = [item['value'] for item in v if item['ov'] and len(item['value']) != 0]
                                    record_dict[k] = config_file["MultipleOVDelimiter"].join(values) if values else None
                                else:
                                    record_dict[k] = i[k1][k][0]['value'] if len(i[k1][k][0]['value']) != 0 else None
                            if k in dict_map["Nested"]:
                                dict_child_struct = {}
                                child_list = []
                                list_of_subattributes = dict_map["Nested"][k]
                                dict_child_struct = {key: None for key in list_of_subattributes}
                                for t in i[k1][k]:
                                    if 'uri' in t:
                                        dict_child_struct["uri"] = t['uri']
                                        dict_child_struct["parent_uri"] = i['uri']
                                    if 'value' in t:
                                        for key in t['value']:
                                            nested_child_dict = {}
                                            nested_child_list = []
                                            if dict_map["Nested"][k][key] == "":
                                                values = [item['value'] for item in t['value'][key] if item['ov'] and len(item['value']) != 0]
                                                dict_child_struct[key] = '|'.join(values) if values else None
                                            else:
                                                for s in t['value'][key]:
                                                    nested_child_dict = {key: None for key in dict_map["Nested"][k][key]}
                                                    if 'value' in s:
                                                        for x, y in s['value'].items():
                                                            if x in dict_map["Nested"][k][key]:
                                                                values = [item['value'] for item in s['value'][x] if item['ov'] and len(item['value']) != 0]
                                                                nested_child_dict[x] = '|'.join(values) if values else None
                                                        nested_child_dict["uri"] = s['uri']
                                                        nested_child_dict["parent_uri"] = t['uri']
                                                        nested_child_dict["grand_parent_uri"] = i['uri']
                                                        nested_child_list.append(nested_child_dict)
                                                nested_child_json_to_csv(k, key, nested_child_list, entity_name)
                                    child_list.append(dict_child_struct)
                                    dict_child_struct = {}
                                child_json_to_csv(k, child_list, entity_name)
                                child_list = []
                                dict_child_struct = {}
                        except Exception as e:
                            logger.error(f"An error occurred while processing the attributes of records: {e}")
            if k1 in dict_map["System_Variables"] and k1 != 'attributes':
                record_dict[k1] = i[k1] if len(str(i[k1])) != 0 else None
        entity_list.append(record_dict)
        return entity_list
    except Exception as e:
        logger.error(f"An error occurred while retrieving entity information: {e}")

# Main function to process JSON files based on a configuration file, transforming data into CSV format and handling nested structures.
def main_entity(configuration_file_path):
    if not configuration_file_path or not os.path.isfile(configuration_file_path):
        print("Invalid configuration file path provided.")
        return
    Record_number = 0

    global config_file
    global timestamp_unix_ms
    try:
        now = datetime.now()
        timestamp_unix_ms = int(now.timestamp() * 1000)
    except Exception as e:
        print(f"An error occurred while generating the timestamp: {e}")
    try:
        with open(configuration_file_path, 'r') as file:
            config_file = json.load(file)
    except Exception as e:
        print(f"An error occurred while opening the configuration file: {e}")
        return

    setup_logging()  # Initialize logging
    logger.info("Starting the entity extraction process.")

    global output_file_path
    entity_name = config_file["EntityType"]
    mapping_file_path = config_file["InputLocationMappingFile"]
    output_file_path = os.path.join(config_file["OutputLocation"], f"Output_Logs_{timestamp_unix_ms}.log")
    try:
        with open(mapping_file_path, 'r') as file:
            global dict_map
            dict_map = json.load(file)
    except Exception as e:
        logger.error(f"An error occurred while opening the mapping file: {e}")
        return

    input_file = config_file["InputLocation"]
    directory = input_file

    global all_data
    all_data = []

    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if filename.endswith(".zip"):
            try:
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    for file in zip_ref.namelist():
                        if file.endswith(".json"):
                            with zip_ref.open(file) as f:
                                load_json(f)
            except Exception as e:
                logger.error(f"An error occurred while processing the ZIP file: {e}")

        elif filename.endswith(".gz"):
            try:
                with gzip.open(filepath, 'rt') as gz_file:
                    load_json(gz_file)
            except Exception as e:
                logger.error(f"An error occurred while processing the GZ file: {e}")

        elif filename.endswith(".json"):
            try:
                with open(filepath, 'r') as json_file:
                    load_json(json_file)
            except Exception as e:
                logger.error(f"An error occurred while processing the JSON file: {e}")

    global entity_list
    record_dict = {}
    for i in all_data:
        if str(entity_name) in str(i["type"]):
            try:
                combined_dict = {**dict_map["System_Variables"], **dict_map["Attributes"]}
                record_dict = dict.fromkeys(combined_dict, '')
                Record_number += 1
                logger.info(f"Processing record number: {Record_number}")
                entity_list = getEntityInfo(entity_list, i, dict_map, record_dict, str(entity_name))
            except KeyError:
                logger.warning(f"The key {entity_name} does not exist in the dictionary.")
                
    csv_file_path = os.path.join(config_file["OutputLocation"], f"{entity_name}_{timestamp_unix_ms}.csv")

    if len(entity_list) != 0:
        try:
            entity_df = pd.DataFrame(entity_list)
            entity_df = change_dataframe_headers(entity_df, dict_map)
            entity_df.to_csv(csv_file_path, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, index=None, na_rep='')
            logger.info(f"Entity CSV file created: {csv_file_path}")
        except Exception as e:
            logger.error(f"An error occurred while writing the DataFrame to a CSV file: {e}")
    else:
        logger.warning("No data to process. Entity type of input data doesn't match with that of the configuration file.")

    logger.info("Entity extraction process completed.")

configuration_file_path = input("Please enter the path to the configuration file: ")
main_entity(configuration_file_path)