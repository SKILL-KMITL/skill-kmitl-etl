import logging
from etl.insert import insert_career_field, insert_career_group, insert_career_positions, insert_profile, insert_skill
from connection import init_connection

def etl_file(cursor, connection, field, df):

  # insert field by each file name
  field_id = insert_career_field(cursor, connection, field)


  # loop thought df row
  current_group = ""
  current_position = ""
  for index, row in df.iterrows():

    # trigger when insert new group
    if current_group != row.group:
      logging.info('Insert group: %s' % (row.group))
      # insert group
      group_id = insert_career_group(cursor, connection, row.group, field_id)
    current_group = row.group
    

    # trigger when insert new position
    if current_position != row.position:
      logging.info('Insert position: %s' % (row.position))
      # insert position
      position_id = insert_career_positions(cursor, connection, row.position, field_id)
    current_position = row.position

    # insert profile
    profile_id = insert_profile(cursor, connection, row, position_id)

    if profile_id: # profile already inserted
      
      #insert skill & position_m_skill
      insert_skill(cursor, connection, row, position_id)
      

    
