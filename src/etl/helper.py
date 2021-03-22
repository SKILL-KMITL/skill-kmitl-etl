def list_to_postgres_array(data):
    tmp = str(data).replace('[', '{').replace(']', '}').replace('\'', '\"').replace('"', '\'').replace("\''", '\'')
    return tmp