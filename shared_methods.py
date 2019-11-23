import json
import urllib
import sqlalchemy as db

import requests


def get_token(base_url, api_login, api_password):
    data_get = {'Username': api_login,
                'Password': api_password}
    r = requests.post(base_url, json=data_get)
    if r.ok:
        auth_token = json.loads(r.text)["Data"]["AccessToken"]
        #         print("Token: " + auth_token)
        return auth_token
    else:
        print("HTTP %i - %s" % (json.loads(r.text)["Code"], json.loads(r.text)["Message"]))


def get_api_response(url, token, json_request):
    head = {'Authorization': 'Bearer {}'.format(token), 'Content-type': 'application/json', 'Accept': 'text/plain'}
    response = requests.post(url, headers=head, data=json_request)
    # return json.loads(response.text)['Data']
    return json.loads(response.text)
    # return response.text


def get_data_from_api_response(json_response):
    return json_response['Data']


def get_code_from_api_response(json_response):
    return json_response['Code']


def get_message_from_api_response(json_response):
    return json_response['Message']


def create_list_from_table(server, database, user_name, password, table_name):
    params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+user_name+";PWD="+password)
    engine = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    metadata = db.MetaData()
    connection = engine.connect()
    table = db.Table(table_name, metadata, autoload=True, autoload_with=engine)
    query = db.select([table])
    result_proxy = connection.execute(query)
    result_set = result_proxy.fetchall()
    return result_set
    # return result_set[0].keys()


def add_to_log(api_url, request_data, response_code, response_message, response_data, server, database, user_name, password):
    params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+user_name+";PWD="+password)
    engine = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    engine.execute("INSERT INTO tbFreightPopLog "
                   "(api_url, request_data, response_code, response_message, response_data) "
                   "VALUES ('{}', '{}', '{}', '{}', '{}')".format(api_url, request_data, response_code, response_message, response_data))

