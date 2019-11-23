import json

import settings
import shared_methods as sm


def main():
    # read settings from config file
    api_login = settings.api_login
    api_password = settings.api_password
    get_token_url = settings.get_token_url
    post_shipment_url = settings.post_shipment_url
    db_server = settings.db_server
    db_database = settings.db_database
    db_user_name = settings.db_user_name
    db_password = settings.db_password
    shipments_table_name = settings.shipments_table_name
    tracking_table_name = settings.tracking_table_name

    # import shipments from db
    shipments = sm.create_list_from_table(db_server, db_database, db_user_name, db_password, shipments_table_name)

    # import tracked containers from db
    tracked_containers = get_tracked_containers(db_server, db_database, db_user_name, db_password, tracking_table_name)

    # create new list that contains only shipments that are not already being tracked
    shipments_to_post = [x for x in shipments if x.TrackingNumber not in tracked_containers]

    # generate one json request for each line and add it to the list
    all_requests = []
    for record in shipments_to_post:
        json_request = create_json(record)
        all_requests.append(json_request)

    # get token
    token = sm.get_token(get_token_url, api_login, api_password)

    # call shipment API and send up to 30 requests since that's the limit per minute
    for i in range(min(49, len(all_requests))):
        # print(all_requests[i])
        shipment_response_json = sm.get_api_response(post_shipment_url, token, all_requests[i])
        data = sm.get_data_from_api_response(shipment_response_json)
        code = sm.get_code_from_api_response(shipment_response_json)
        message = sm.get_message_from_api_response(shipment_response_json)
        # add to log
        sm.add_to_log(post_shipment_url, str(all_requests[i]).replace("'", "\""), code, message, str(data).replace("'", "\""), db_server, db_database, db_user_name, db_password)


def get_tracked_containers(db_server, db_database, db_user_name, db_password, tracking_table_name):
    tracked_containers = sm.create_list_from_table(db_server, db_database, db_user_name, db_password, tracking_table_name)
    containers = []
    for record in tracked_containers:
        containers.append(record.TrackingNumber)
    return containers


def create_json(record):
    record_object = {"CompanyCarrierId": record.CompanyCarrierId,
                    "TrackingNumbers": record.TrackingNumber,
                    "ShipDate": record.ShipDate,  # "2019-11-10T14:45:33.6011403-08:00"
                    "ShipmentFromAddress": {
                        "AttentionTo": "",
                        "City": record.ShipFrom_City,
                        "CompanyId": "",
                        "Company": "",
                        "Country": record.ShipFrom_Country,
                        "Email": "",
                        "State": record.ShipFrom_State,
                        "Street1": record.ShipFrom_Street1,
                        "Street2": "",
                        "Phone": "",
                        "Zip": record.ShipFrom_Zip
                    },
                    "ShipmentToAddress": {
                        "AttentionTo": "",
                        "City": record.ShipTo_City,
                        "CompanyId": "",
                        "Company": "",
                        "Country": record.ShipTo_Country,
                        "Email": "",
                        "State": record.ShipTo_State,
                        "Street1": record.ShipTo_Street1,
                        "Street2": "",
                        "Phone": "",
                        "Zip": record.ShipTo_Zip
                    },
                    "EstimatedDeliveryDate": "",
                    "Items": [
                        {
                            "PackageQuantity": record.PackageQuantity,
                            "PackageType": record.PackageType,
                            "Height": record.Height,
                            "Length": record.Length,
                            "TotalWeight": record.TotalWeight,
                            "Width": record.Width
                        }
                    ],
                    "ItnNumber": "",
                    "TrackNote": "",
                    "UserDefinedField": "",
                    "Cost": "",
                    "Service": record.ServiceID,
                    "Carrier": record.CarrierName,
                    "ReferenceOne": record.ReferenceOne,
                    "ReferenceTwo": record.ReferenceTwo,
                    "Reference3": "",
                    "Reference4": "",
                    "Reference5": "",
                    "Reference6": record.Reference6,
                    "OrderNumber": "",
                    "Payer": ""
                    }
    return json.dumps(record_object, default=str)


if __name__ == '__main__':
    main()