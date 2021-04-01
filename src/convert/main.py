import os
import pandas as pd
import json
import numpy as np
import logging
from logging.handlers import RotatingFileHandler
from logging import handlers


# init logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log", mode='w'),
        logging.StreamHandler()
    ]
)

# Get list of file from path
def readDir(path=None): 
  return [f for f in listdir(path) if isfile(join(path, f))]

# Create list from read files
def readFiles(path=None, files=None): 
  return [pd.read_json(path + f) for f in files]

# scan file, dir in directory
def scanDir(path=None):
  item = os.listdir(path)
  item = [x for x in item if not "[" in x] # ignore skip file [n]
  item = [x for x in item if not ".gitkeep" in x] # ignore .gitkeep

  if (len(item) > 0):
    return item
  else:
    return False

# unique profile
def unique_profile(data, reset_index):
  data = data.drop_duplicates(subset = ["url"])
  if (reset_index):
      data = data.reset_index(drop=True)
  return data



def startup():
  raw_root = os.getcwd() # set execute path
  raw_path = os.path.join(raw_root, 'data/raw') # set raw data path
  field_dir = scanDir(raw_path) # field directory name
  
  if (not field_dir):
    logging.warning('Raw directory is empty')

  # Iterate field
  for field in field_dir:
    field_path = raw_path + "/" + field
    group_dir = scanDir(field_path) # scan directory in field directory
    store = []

    if (not group_dir):
      logging.warning('Field "' + field + '" directory is empty -- SKIP')
      continue

    for group in group_dir:
      group_path = field_path + "/" + group
      position_dir = scanDir(group_path) # scan file in group directory

      if (not position_dir):
        logging.warning('Group "' + group + '" directory is empty -- SKIP')
        continue
      
      # Read position files
      position_data = []
      for file in position_dir:

        try:
          position_path = group_path + "/" + file
          with open(position_path, encoding="utf8") as json_data:
            position_data.append(json.load(json_data))
        except (FileNotFoundError) as e:
          logging.warning('Position "' + file + '" file not found -- SKIP') 
        except (PermissionError) as e:
          logging.warning('Position "' + file + '" permission denied -- SKIP')

      tmp_positions = []
      tmp_task = ""
      tmp_desc = ""
      for position in position_data:
        for item in position['data']:

          # Assign group, position
          item['group'] = position['group']
          item['position'] = position['position'].replace(".json", "")

          # Assign tasks
          if (not 'tasks' in position):
            item['tasks'] = []
          else:
            if (tmp_task != position['tasks']):
              item['tasks'] = position['tasks'] 
              tmp_task = position['tasks']

          # Assign desc
          if (not 'desc' in position):
            item['desc'] = ''
          else:
            if (tmp_desc != position['desc']):
              item['desc'] = position['desc'] 
              tmp_desc = position['desc']

          tmp_positions.append(item)
      
      df_positions = pd.DataFrame(tmp_positions)

      # unique profile
      profile = unique_profile(df_positions, True)

      # re-create skill
      profile['skill_hard'] = profile['skill'].apply(lambda row: [list(skills.values())[0][0] for skills in row if list(skills.keys())[0] not in  ["interpersonal skills", "languages"]])
      profile['skill_soft'] = profile['skill'].apply(lambda row: [list(skills.values())[0][0] for skills in row if list(skills.keys())[0] in ["interpersonal skills", "languages"]])
      profile['skill_hard'] = profile['skill_hard'] + profile['skill_top'] # assign top skill into hard skill

      # validate nan
      profile = profile.dropna(subset = ['skill'])
      profile = profile.replace(np.nan, '', regex=True)
      profile = profile[profile['skill'] != 0]
      profile = profile.fillna('')
      profile = profile.reset_index(drop=True)
      store.append(profile)

    store = pd.concat(store)
    print(field, len(store))
    output_dir = raw_root + "/" + "data/source"
    output_path = output_dir + "/" + field + ".h5"
    os.makedirs(output_dir, exist_ok=True)
    store.to_hdf(output_path, 'df', complib='zlib',complevel=6)
startup()