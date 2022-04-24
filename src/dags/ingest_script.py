import pandas as pd
import datetime
import json
import os
from sqlalchemy import create_engine
from jsonschema import validate, Draft7Validator, FormatChecker
from dateutil.parser import isoparse
from json.decoder import JSONDecodeError

def validate_schema(json_dict:dict):
    # check correct attribute types, no unexpected or missing attributes, found in list of valid values, coordinates > 0, 
    job_schema = {
        "type": "object",
        "additionalProperties":False,
        "properties": {
            "job_id": {"type":"string"},
            "driver_id": {"type":"string"},
            "start_time":{"type":"string", "format":"date-time"},
            "start_state":{"type":"string", "enum":["LOADED","UNLOADED"]},
            "start_coordinate":{"type":"array", "minItems": 2, "maxItems":2, "items":{
                "additionalProperties": False,
                "properties": {
                        "type": "float",
                    }
                }
            },
            "end_time":{"type":"string","format":"date-time"},
            "end_state":{"type":"string","enum":["LOADED","UNLOADED"]},
            "end_coordinate":{"type":"array", "minItems": 2, "maxItems":2, "items":{
                "additionalProperties": False,
                "properties": {
                        "type": "float",
                    }
                }
            }
        },
        "patternProperties":{
            "^(leg_[0-9]+)+$":{
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "arrive_time":{"type": "string","format":"date-time"},
                    "type":{"type":"string", "enum":["LOAD","UNLOAD"]},
                    "leave_time":{"type": "string", "format":"date-time"},
                    "coordinate":{"type":"array", "minItems": 2, "maxItems":2, "items":{
                        "additionalProperties": False,
                        "properties": {
                                "type": "float",
                            }
                        },
                    }
                },
                "required":[
                    "arrive_time",
                    "type",
                    "leave_time",
                    "coordinate"
                ]
            }
        },
        "required":[
                    "job_id",
                    "driver_id",
                    "start_time",
                    "start_state",
                    "start_coordinate",
                    "end_time",
                    "end_state",
                    "end_coordinate",
                    "leg_1",
                ]
    }

    validator = Draft7Validator(job_schema, format_checker=FormatChecker())
    try:
        outcome = validator.validate(json_dict)
    except Exception as e:
        print(e)
        return False
    return True

def validate_legs(json_dict:dict):
    # validate leg sequence, no missing legs in between, no repeated legs, legs suffix between 1 to 20
    keys_legs = [int(x.split("_")[1]) for x in list(json_dict.keys()) if x[:3] == "leg"]
    if len(keys_legs) > 20 or sorted(keys_legs) != list(range(1,len(keys_legs)+1)):
        return False
    return True

def validate_timings(json_dict:dict):
    # validate leave(n-1) < arrive(n), start_time <= arrive(0), leave(-1) >= end_time 
    keys_legs = [int(x.split("_")[1]) for x in list(json_dict.keys()) if x[:3] == "leg"]
    keys_legs = sorted(keys_legs)
    start_time, end_time = json_dict['start_time'], json_dict['end_time']
    # print(isoparse(start_time), isoparse(end_time))
    if isoparse(start_time) > isoparse(end_time):
        return False
    for leg in keys_legs:
        k = "leg_" + str(leg)
        if leg == 1:
            if isoparse(start_time) > isoparse(json_dict[k]['arrive_time']):
                return False
        elif leg == len(keys_legs):
            if isoparse(end_time) < isoparse(json_dict[k]['leave_time']):
                return False
        else:
            if isoparse(json_dict[k]['leave_time']) > isoparse(json_dict["leg_"+str(leg+1)]['arrive_time']):
                return False
    return True

def validate_loadings(json_dict:dict):
    if json_dict['start_state'] == "UNLOADED" and json_dict['leg_1']['type'] == "UNLOAD": #unloaded but unloading at leg_1
            return False
    return True

def process_jobs(base_dir:str):
    incoming_folder_dir = base_dir + "incoming/"
    incoming_file_list = os.listdir(incoming_folder_dir)
    incoming_file_list_txt = [x for x in list(incoming_file_list) if x[-4:] == ".txt"]
    # print(incoming_file_list_txt)
    if len(incoming_file_list_txt) > 0:
        new_bad_lines_dir = base_dir + "processed/new_bad_lines.txt"
        new_good_lines_dir = base_dir + "processed/new_good_lines.txt"
        new_bad_lines = open(new_bad_lines_dir,"w")
        new_good_lines = open(new_good_lines_dir,"w")
        for file_str in [x for x in incoming_file_list_txt if x != new_bad_lines_dir and x!= new_good_lines_dir]:
            # print(file_str)
            f = open(incoming_folder_dir + file_str,"r")
            lines = f.readlines()
            for line in lines:
                print(line)
                errorType = ""
                while True:
                    is_good_json, is_good_schema, is_good_legs, is_good_timings, is_good_loadings = True, True, True, True, True
                    try:
                        json_dict = json.loads(line)
                    except JSONDecodeError:
                        is_good_json = False
                        errorType = "syntax"
                        break
                    
                    is_good_schema = validate_schema(json_dict)
                    if not is_good_schema:
                        errorType = "schema"
                        break

                    is_good_legs = validate_legs(json_dict) 
                    if not is_good_legs:
                        errorType = "legs"
                        break

                    is_good_timings = validate_timings(json_dict)
                    if not is_good_timings:
                        errorType = "timings"
                        break

                    is_good_loadings = validate_loadings(json_dict) 
                    if not is_good_loadings:
                        errorType = "loading"
                    break

                if is_good_json and is_good_schema and is_good_legs and is_good_timings:
                    new_good_lines.write(line)
                else:
                    print(errorType, line)
                    new_bad_lines.write(line)
            f.close()
            os.remove(incoming_folder_dir + file_str)
        new_bad_lines.close()
        new_good_lines.close()
        return 'archive_records'
    else:
        return 'end_run'

def archive_records(base_dir:str, new_good_lines_dir:str, new_bad_lines_dir:str):
    bad_master_dir = base_dir + "bad/bad_lines.txt"
    good_master_dir = base_dir + "good/good_lines.txt"
    # archive records to good and bad
    if os.path.isfile(base_dir + new_bad_lines_dir):
        if os.path.isfile(bad_master_dir):
            print("bad exists")
            bad_master = open(bad_master_dir,"a")
            bad_new = open(base_dir + new_bad_lines_dir,"r")
            lines=bad_new.readlines()
            for line in lines:
                bad_master.write(line)
            bad_new.close()
            bad_master.close()
        else:
            print('no bad lines')
            os.rename(base_dir + new_bad_lines_dir, bad_master_dir)

    if os.path.isfile(base_dir + new_good_lines_dir):
        if os.path.isfile(good_master_dir):
            print("good exists")
            good_master = open(good_master_dir,"a")
            good_new = open(base_dir + new_good_lines_dir,"r")
            lines=good_new.readlines()
            for line in lines:
                good_master.write(line)
            good_new.close()
            good_master.close()
        else:
            print('no good lines')
            os.popen("cp " + base_dir + new_good_lines_dir + " " + good_master_dir)
    
    return base_dir + new_good_lines_dir

def load_good_jobs(new_good_lines_dir:str):
    jobs_df = pd.DataFrame()
    legs_df = pd.DataFrame()
    if os.path.isfile(new_good_lines_dir):
        with open(new_good_lines_dir, "r") as good_jobs:
            lines = good_jobs.readlines()
            for line in lines:
                f = json.loads(line)
                f['start_xcoord'] = f['start_coordinate'][0]
                f['start_ycoord'] = f['start_coordinate'][1]
                f['end_xcoord'] = f['end_coordinate'][0]
                f['end_ycoord'] = f['end_coordinate'][1]
                f.pop("start_coordinate")
                f.pop("end_coordinate")
                keys_legs = [int(x.split("_")[1]) for x in list(f.keys()) if x[:3] == "leg"]
                for leg in keys_legs:
                    legs_dict = dict(job_id=f['job_id'], leg=leg, 
                        leg_id=f['job_id'] + "_" + str(leg),
                        arrive_time=f["leg_"+str(leg)]['arrive_time'], 
                        type=f["leg_"+str(leg)]['type'], 
                        leave_time=f["leg_"+str(leg)]['leave_time'], 
                        xcoord=f["leg_"+str(leg)]['coordinate'][0],
                        ycoord=f["leg_"+str(leg)]['coordinate'][1],
                        )
                    f.pop("leg_"+str(leg))
                    print(legs_dict)
                    legs_df = pd.concat([legs_df, pd.DataFrame.from_dict([legs_dict])])
                new_df = pd.DataFrame.from_dict([f])
                print(new_df)
                jobs_df = pd.concat([jobs_df, new_df])
            good_jobs.close()
        jobs_df.reset_index(inplace=True,drop=True)
        legs_df.reset_index(inplace=True,drop=True)
        jobs_df.set_index('job_id')
        legs_df.set_index('job_id')
        #jobs_df.to_csv("./data/temp1.csv")
        #legs_df.to_csv("./data/temp2.csv")
        load_df_to_db(jobs_df,"jobs")
        load_df_to_db(legs_df,"legs")
        os.remove(new_good_lines_dir)
    

def load_df_to_db(df:pd.DataFrame,table_name:str):
    PG_USER = os.getenv('POSTGRES_USER')
    PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    PG_PORT = '5432'
    PG_DATABASE = os.getenv('POSTGRES_DB')
    print("credentials:", PG_USER, PG_PASSWORD, PG_PORT, PG_DATABASE)
    engine = create_engine('postgresql://{user}:{password}@{svc_name}:{port}/{database}'.format(user=PG_USER,password=PG_PASSWORD,svc_name='postgres',port=PG_PORT,database=PG_DATABASE))
    # engine = create_engine('postgresql://airflow:airflow@0.0.0.0:5432/airflow')
    conn = engine.connect()
    current_jobs_query = "SELECT job_id FROM " + table_name
    current_jobs = pd.read_sql_query(current_jobs_query, conn)
    df = df[~df['job_id'].isin(current_jobs['job_id'])] # remove duplicate job_id entries
    if len(df.index) > 0:
        df.to_sql(table_name,con=engine,if_exists='append',index=False)
    else:
        print("imported jobs are found in database.")
    return