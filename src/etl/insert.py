import logging
import psycopg2
from etl.helper import list_to_postgres_array
import re

def insert_career_field(cursor, connection, field):
    field = field.replace('.h5', '')
    logging.info('Insert field: %s' % (field))
    query = f"\
          INSERT INTO career_fields (name) SELECT ('{field}') \
          WHERE NOT EXISTS (SELECT id FROM career_fields WHERE name = '{field}') \
          RETURNING id \
          "
    try:
        cursor.execute(query)
        connection.commit()

        if cursor.rowcount:
            return cursor.fetchone()[0]
        else:
            query = f"SELECT id FROM career_fields WHERE name='{field}'"
            cursor.execute(query)
            connection.commit()
            return cursor.fetchone()[0]
    except (Exception, psycopg2.Error) as error:
        logging.error('Failed to insert `career_fields`: %s' % error)
        raise SystemError("Failed to insert `career_fields`", error)
        exit(1)


def insert_career_group(cursor, connection, group, field_id):
    query = f"\
      INSERT INTO career_groups (name, career_field_id) SELECT '{group}', {field_id} \
      WHERE NOT EXISTS (SELECT name FROM career_groups WHERE name='{group}') \
      RETURNING id \
      "
    try:
        cursor.execute(query)
        connection.commit()
        
        # check inserted
        if cursor.rowcount:
            return cursor.fetchone()[0]
        else:
            query = f"SELECT id FROM career_groups WHERE name='{group}'"
            cursor.execute(query)
            connection.commit()
            return cursor.fetchone()[0]
    except (Exception, psycopg2.Error) as error:
        logging.error('Failed to insert `career_groups`: %s' % error)
        raise SystemError("Failed to insert `career_groups`", error)
        exit(1)


def insert_career_positions(cursor, connection, position, field_id):
    query = f"\
        INSERT INTO career_positions (name, career_group_id) SELECT '{position}', {field_id} \
        WHERE NOT EXISTS (SELECT name FROM career_positions WHERE name='{position}') \
        RETURNING id \
        "
    try:
        cursor.execute(query)
        connection.commit()
        
        # check inserted
        if cursor.rowcount:
            return cursor.fetchone()[0]
        else:
            query = f"SELECT id FROM career_positions WHERE name='{position}'"
            cursor.execute(query)
            connection.commit()
            return cursor.fetchone()[0]
        
    except (Exception, psycopg2.Error) as error:
        logging.error('Failed to insert `career_positions`: %s' % error)
        raise SystemError("Failed to insert `career_positions`", error)
        exit(1)

def insert_profile(cursor, connection, user, position_id):
    # in case column education not exist
    if "education" in user:
        education = list_to_postgres_array(user.education)
    else:
        education = []
    
    exp = str(user.exp).replace('\'', '\"').replace('"', '\'').replace("\''", '\'')
    skill_hard = list_to_postgres_array(user.skill_hard)
    skill_soft = list_to_postgres_array(user.skill_soft)
    interest = list_to_postgres_array(user.interest)
    
    query = f"""\
        INSERT INTO profiles (url, about, exp, skill_hard, skill_soft, career_position_id, interest, education) \
        SELECT %s, %s, %s, %s, %s, %s, %s, %s\
        WHERE NOT EXISTS (SELECT id FROM profiles WHERE url = %s)
        RETURNING id \
        """
    
    try:
        cursor.execute(query, (user.url, user.about, exp, skill_hard, skill_soft, position_id, interest, education, user.url))
        connection.commit()
        if cursor.rowcount:
            logging.debug('Insert profile: %s' % (user.url))
            return cursor.fetchone()[0]
        else:
            logging.debug('Skip insert profile: %s' % (user.url))
            return False
        
    except (Exception, psycopg2.Error) as error:
        logging.error('Failed insert profile [SKIP]: %s' % error)
        connection.rollback()
        return False


def insert_skill(cursor, connection, data, position_id):
    skill_hard = [re.sub(r'[^A-Za-z0-9 ]+', '', skill) for skill in data.skill_hard if len(data.skill_hard)]
    skill_soft = [re.sub(r'[^A-Za-z0-9 ]+', '', skill) for skill in data.skill_soft if len(data.skill_soft)]
    
    insert_skill_commit(cursor, connection, skill_hard, 'Hard Skill')
    insert_skill_commit(cursor, connection, skill_soft, 'Soft Skill')
    insert_position_m_skill(cursor, connection, skill_hard, position_id)
    insert_position_m_skill(cursor, connection, skill_soft, position_id)

def insert_skill_commit(cursor, connection, data, skill_type):
    insert_skill_id = []
    for skill in data:
        skill = skill
        query = f"\
                INSERT INTO skills (name, type) SELECT '{skill}', '{skill_type}' \
                WHERE NOT EXISTS (SELECT name FROM skills WHERE name='{skill}') \
                RETURNING id \
                "
        try:
            cursor.execute(query)
            connection.commit()

            # check inserted
            if cursor.rowcount:
                insert_skill_id.append(cursor.fetchone()[0])
            else:
                query = f"SELECT id FROM skills WHERE name='{skill}'"
                cursor.execute(query)
                connection.commit()
                insert_skill_id.append(cursor.fetchone()[0])

        except (Exception, psycopg2.Error) as error:
            print("Failed to insert `skill`", error)
            
    return insert_skill_id

def insert_position_m_skill(cursor, connection, data, position_id):
    if (len(data)): # validate data is exist
        for skill in data:
            
            # get skill_id
            query = f"SELECT id FROM skills WHERE name='{skill}'"
            cursor.execute(query)
            connection.commit()
            skill_id = cursor.fetchone()[0]
            
            
            query = f"SELECT id FROM position_m_skill WHERE position_id='{position_id}' AND skill_id='{skill_id}'"
            cursor.execute(query)
            connection.commit()
            
            # if duplicate plus amount
            if cursor.rowcount:
                query = f"UPDATE position_m_skill SET amount=amount+1 WHERE position_id='{position_id}' AND skill_id='{skill_id}'"
            else:
                query = f"INSERT INTO position_m_skill (position_id, skill_id) VALUES ({position_id}, {skill_id})"
                
            cursor.execute(query)
            connection.commit()
        return True
    else:
        return False