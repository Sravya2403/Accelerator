#Entity Extract Utility
#Author : Raj Kumar Panjiyara , MD Sarfaraz Hussain
import zipfile
import gzip
import json
import uuid
import datetime
import streamlit as st
from datetime import datetime
from datetime import timedelta
import pandas as pd
import csv
import os
import sys

all_data=[]
output_file_path=''
entity_list=[]

#Generates the current timestamp in the format `YYYY-MM-DD HH:MM:SS,mmm
def get_timestamp():
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    return timestamp

#Appends a sentence with the current timestamp to a specified file
def write_sentence_to_file(output_file_path, sentence_to_write):
    with open(output_file_path, 'a') as file:
        sentence_to_write = "Current TimeStamp: " + str(get_timestamp()) + "   " + str(sentence_to_write)
        file.write(sentence_to_write + "\n")


#Loads JSON data from a file and appends it to a global list all_data
def load_json(file):
    try:
        data = json.load(file)
        all_data.extend(data)
    except Exception as e:
        print(f"An error occurred while loading JSON data: {e}")
    return all_data
 
#Converts nested child JSON records to a CSV file and writes them to a specified location 
def nested_child_json_to_csv(k,key,nested_child_list,entity_name):
    try:
        sentence_to_write = "Nested Child records transformation from json to csv for " + str(k) + str(key)
        write_sentence_to_file(output_file_path, sentence_to_write)
        entity_nested_child_df = pd.DataFrame(nested_child_list)
        nested_child_name = entity_name + "_" + str(k) + "_" + str(key) +'_Records_' + str(timestamp_unix_ms)
        csv_file_path_child = config_file["OutputLocation"]
        csv_file_path_child = str(csv_file_path_child) + "/" + nested_child_name + ".csv"
        
        if os.path.isfile(csv_file_path_child):
            entity_nested_child_df.to_csv(csv_file_path_child, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, mode='a', header=False, index=None, na_rep='')
        else:
            entity_nested_child_df.to_csv(csv_file_path_child, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, index=None, na_rep='')
                
    except Exception as e:
        print("An error occurred. Check the Log file for detail")
        sentence_to_write = f"An error occurred while transforming nested child records from JSON to CSV: {e}"
        write_sentence_to_file(output_file_path, sentence_to_write)

#Converts child JSON records to a CSV file and writes them to a specified location
def child_json_to_csv(k,child_list,entity_name):
    try:
        sentence_to_write = "Child records transformation from json to csv for " + str(k)
        write_sentence_to_file(output_file_path, sentence_to_write)
        entity_child_df = pd.DataFrame(child_list)
        child_name = entity_name + "_" + str(k) + '_Records_' + str(timestamp_unix_ms)
        csv_file_path_child = config_file["OutputLocation"]
        csv_file_path_child = str(csv_file_path_child) + "/" + child_name + ".csv"
        
        if os.path.isfile(csv_file_path_child):
            entity_child_df.to_csv(csv_file_path_child, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, mode='a', header=False, index=None, na_rep='')
        else:
            entity_child_df.to_csv(csv_file_path_child, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, index=None, na_rep='')
                
    except Exception as e:
        print("An error occurred. Check the Log file for detail")
        sentence_to_write = f"An error occurred while transforming child records from JSON to CSV: {e}"
        write_sentence_to_file(output_file_path, sentence_to_write)

#Processes entity information from JSON data, handling attributes and nested structures, and appends the processed data to entity_list.
def getEntityInfo(entity_list,i,dict_map,record_dict,entity_name):    
    child_list = []
    dict_child_struct = {}
    nested_child_dict = {}
    nested_child_list = []
    try:
        for k1, v1 in i.items():
            if k1 == 'attributes':
                if str(v1) != "{}":
                    for k,v in i[k1].items():
                        try:
                            if k in dict_map["Attributes"]:
                                #made changes for ov="True"
                                values = []
                                for item in v:
                                    if item['ov'] == True and len(item['value']) != 0:
                                        values.append(item['value'])
                                record_dict[k] = '|'.join(values) if values else None
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
                                                #made changes for ov="True"
                                                values = []
                                                for item in t['value'][key]:
                                                    if item['ov'] == True and len(item['value']) != 0:
                                                        values.append(item['value'])
                                                dict_child_struct[key] = '|'.join(values) if values else None
                                            else:
                                                for s in t['value'][key]:
                                                    nested_child_dict = {}
                                                    if 'value' in s:
                                                        for x,y in s['value'].items():
                                                            if x in dict_map["Nested"][k][key]:
                                                                #made changes for ov="True"
                                                                values = []
                                                                for item in s['value'][x]:
                                                                    if item['ov'] == True and len(item['value']) != 0:
                                                                        values.append(item['value'])
                                                                nested_child_dict[x] = '|'.join(values) if values else None
                                                        nested_child_dict["parent_uri"] = t['uri']
                                                        nested_child_dict["grand_parent_uri"] = i['uri']
                                                        nested_child_list.append(nested_child_dict)
                                                nested_child_json_to_csv(k,key,nested_child_list,entity_name)
                                    child_list.append(dict_child_struct)
                                    dict_child_struct = {}
                                child_json_to_csv(k,child_list,entity_name)
                                child_list = []
                                dict_child_struct = {}
                        except Exception as e:
                            print("An error occurred. Check the Log file for detail")
                            sentence_to_write = f"An error occurred while processing the attributes of records: {e}"
                            write_sentence_to_file(output_file_path, sentence_to_write)
            if k1 in dict_map["System_Variables"] and k1 != 'attributes':
                record_dict[k1] = i[k1] if len(str(i[k1]))!=0 else None
        entity_list.append(record_dict)
        return entity_list
    except Exception as e:
        print("An error occurred. Check the Log file for detail")
        sentence_to_write = f"An error occurred while retrieving entity information: {e}"
        write_sentence_to_file(output_file_path, sentence_to_write)

#Main function to process JSON files based on a configuration file, transforming data into CSV format and handling nested structures. It also manages logging and error handling
def main_entity(configuration_file_path):
    Record_number = 0

    global config_file
    config_file=configuration_file_path
    global timestamp_unix_ms
    try:
        now = datetime.now()
        timestamp_unix_ms = int(now.timestamp() * 1000)
    except Exception as e:
        print(f"An error occurred while generating the timestamp: {e}")

    global output_file_path
    entity_name = config_file["EntityType"]
    mapping_file_path = config_file["InputLocationMappingFile"]
    output_file_path = config_file["OutputLocation"]
    output_file_path = output_file_path + "/" + "Output_Logs" + "_" + str(timestamp_unix_ms) + ".txt"
    try:
        with open(mapping_file_path, 'r') as file:
            dict_map = json.load(file)
    except Exception as e:
        print(f"An error occurred while opening the mapping file: {e}")

    input_file = config_file["InputLocation"]
    directory = input_file

    global all_data
    all_data=[]

        
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
                print(f"An error occurred while processing the ZIP file: {e}")

        elif filename.endswith(".gz"):
            try:
                with gzip.open(filepath, 'rt') as gz_file:
                    load_json(gz_file)
            except Exception as e:
                print(f"An error occurred while processing the GZ file: {e}")

        elif filename.endswith(".json"):
            try:
                with open(filepath, 'r') as json_file:
                    load_json(json_file)
            except Exception as e:
                print(f"An error occurred while processing the JSON file: {e}")

    global entity_list
    record_dict={}
    for i in all_data:
        if str(entity_name) in str(i["type"]):
            try:
                combined_dict = {**dict_map["System_Variables"], **dict_map["Attributes"]}
                record_dict = combined_dict.copy()
                Record_number = Record_number + 1
                print("Record_number: " + str(Record_number))
                sentence_to_write = "Record_number: " + str(Record_number)
                write_sentence_to_file(output_file_path, sentence_to_write)
                entity_list = getEntityInfo(entity_list,i,dict_map,record_dict,str(entity_name))
            except KeyError:
                sentence_to_write = f"The key {entity_name} does not exist in the dictionary."
                write_sentence_to_file(output_file_path, sentence_to_write)
                
    csv_file_path = config_file["OutputLocation"]
    csv_file_path = str(csv_file_path) + "/" + str(entity_name) + "_" + str(timestamp_unix_ms) + ".csv"

    if len(entity_list) != 0:
        try:
            entity_df = pd.DataFrame(entity_list)
            entity_df.to_csv(csv_file_path, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, index=None, na_rep='')
        except Exception as e:
            print("An error occurred. Check the Log file for detail")
            sentence_to_write = f"An error occurred while writing the DataFrame to a CSV file: {e}"
            write_sentence_to_file(output_file_path, sentence_to_write)
    else:
        try:
            combined_dict = {**dict_map["System_Variables"], **dict_map["Attributes"]}
            record_dict = combined_dict.copy()
            entity_list.append(record_dict)
            entity_df = pd.DataFrame(columns=record_dict)
            entity_df.to_csv(csv_file_path, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, index=None, na_rep='')
            print("No data to process.")
            sentence_to_write = "Entity type of input data does'nt match with that of configuration file. No data to process." 
            write_sentence_to_file(output_file_path, sentence_to_write)
            sentence_to_write = f"The file for '{entity_name}' has been created in the directory."
            write_sentence_to_file(output_file_path, sentence_to_write)
        except Exception as e:
            print("An error occurred. Check the Log file for detail")
            sentence_to_write = f"An error occurred while writing the DataFrame to a CSV file: {e}"
            write_sentence_to_file(output_file_path, sentence_to_write)
            
    try:
        for key in dict_map['Nested']:
            headers = dict_map['Nested'][key]
            directory = config_file["OutputLocation"]
            file_name = str(entity_name) + "_" + str(key) + "_" + "Records" + "_" + str(timestamp_unix_ms) + ".csv"
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                sentence_to_write = "The output files for child tables are created"
                write_sentence_to_file(output_file_path, sentence_to_write)
            else:
                sentence_to_write = f"The file '{file_name}' does not exist in the directory '{directory}'. Creating the file..."
                write_sentence_to_file(output_file_path, sentence_to_write)
                df = pd.DataFrame(columns=headers)
                df.to_csv(file_path, sep=",", escapechar="\\", quoting=csv.QUOTE_ALL, index=None, na_rep='')
                sentence_to_write = f"The file '{file_name}' has been created in the directory '{directory}'."
                write_sentence_to_file(output_file_path, sentence_to_write)
    except Exception as e:
        print("An error occurred. Check the Log file for detail")
        sentence_to_write = f"An error occurred: {e}"
        write_sentence_to_file(output_file_path, sentence_to_write)

    entity_list=[]
    print("The DataFrame was successfully converted to a CSV file.")
    sentence_to_write = "The DataFrame was successfully converted to a CSV file."
    write_sentence_to_file(output_file_path, sentence_to_write)



