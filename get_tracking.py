import settings
import json
import urllib
import pandas as pd
from sqlalchemy import create_engine

import shared_methods as sm


def main():
    # read settings from config file
    api_login = settings.api_login
    api_password = settings.api_password
    get_token_url = settings.get_token_url
    get_tracking_url = settings.get_tracking_url
    db_server = settings.db_server
    db_database = settings.db_database
    db_user_name = settings.db_user_name
    db_password = settings.db_password
    tracking_table_name = settings.tracking_table_name
    shipments_table_name = settings.shipments_table_name

    # get token
    token = sm.get_token(get_token_url, api_login, api_password)

    # call tracking API and store json response in data frame
    container_list = get_container_list(db_server, db_database, db_user_name, db_password, shipments_table_name, tracking_table_name)
    json_request = create_json(container_list)
    tracking_response_json = sm.get_api_response(get_tracking_url, token, json_request)
    # print(tracking_response_json)

    data = sm.get_data_from_api_response(tracking_response_json)
    code = sm.get_code_from_api_response(tracking_response_json)
    message = sm.get_message_from_api_response(tracking_response_json)

    # add to log
    sm.add_to_log(get_tracking_url, str(json_request).replace("'", "\""), code, message, str(data).replace("'", "\""), db_server, db_database, db_user_name, db_password)

    tracking_df = pd.DataFrame.from_dict(data, orient='columns')
    # remove Details column from df since it has nested data we don't need
    tracking_df.drop(columns=['Details'], inplace=True)

    # save df to database table
    create_table_from_df(db_server, db_database, db_user_name, db_password, tracking_table_name, tracking_df)


def create_table_from_df(server, database, user_name, password, table_name, df):
    params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+user_name+";PWD="+password)
    con = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    df.to_sql(table_name, con, if_exists='replace', index=False)


def get_container_list(db_server, db_database, db_user_name, db_password, shipments_table_name, tracking_table_name):
    shipments = sm.create_list_from_table(db_server, db_database, db_user_name, db_password, shipments_table_name)
    tracked_containers = sm.create_list_from_table(db_server, db_database, db_user_name, db_password, tracking_table_name)
    containers = []
    for record in shipments:
        containers.append(record.TrackingNumber)
    for record in tracked_containers:
        if record.TrackingNumber not in containers:
            containers.append(record.TrackingNumber)
    return containers


def create_json(container_list):
    record_object = {'TransactionNumbers': container_list, 'TransactionType': 2}
    return json.dumps(record_object, default=str)


if __name__ == '__main__':
    main()
