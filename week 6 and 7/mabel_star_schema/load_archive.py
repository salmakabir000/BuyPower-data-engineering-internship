# # functions to write 

# # read archive 
# #   some of the modules required for this task-pandas
#    now how to use pandas for this task
#       use string to read the data
# # # flatten json
# # resolve actor
# #resolve repo
# # resolve event type
# #resolve date 
# # build fact records 
# #save fact
import pandas as pd
import json



#read the json so the json_normalize can flatten it 
# def read_archive():
with open("2024-01-15-12.json", "r") as f:
    data =json.load(f)
    print(type(data))    


# def flatten_json():
#     df = pd.json_normalize(data)
#     return df

# def resolve_actor():
#    return

# def resolve_repo():
#    return

# def resolve_event_type():
#    return
   
# def resolve_date():
#    return

# def build_fact_records():
#    return 

# def save_fact():
#    return

