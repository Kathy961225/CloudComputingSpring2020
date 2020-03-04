from botocore.vendored import requests
import json
import os
import random
import boto3

# --------------------------------- SQS Part ---------------------------------
def get_sqs_service():
    sqs = boto3.client('sqs')
    queue_url = 'https://us-east-1.amazonaws.com/135675965852/UserInfo'
    # Get receive information
    input_info = sqs.receive_message(
        QueueUrl = queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    del_sqs_service(input_info)
    return input_info

def del_sqs_service(input_info):
    sqs = boto3.client('sqs')
    information = input_info['Messages'][0]
    receipt_handle = information['ReceiptHandle']
    queue_url = 'https://us-east-1.amazonaws.com/135675965852/UserInfo'
    sqs.delete_message(
        QueueUrl = queue_url,
        ReceiptHandle = receipt_handle
    )

# --------------------------------- Elastic Service Part ---------------------------------
def get_es_service(input_info):
    es_url = 'https://search-restaurants-e3eoppeoaezsmv7mtfavlopz5e.us-east-2.es.amazonaws.com/restaurants/_search?'
    headers = {"Content-Type": "application/json"}
    # Parse the input information
    input_cuisine = input_info['Messages'][0]['MessageAttributes']['Cuisine']['StringValue']
    # Set query data
    es_query = {
        "size": 100,
        "query": {
            "query_string": {
                "default_field": "Cuisine",
                "query": input_cuisine
                }
            }
        }
    # Request to the elastic service
    es_response = requests.get(es_url, headers = headers, data = json.dumps(es_query))
    return es_response.json()['hits']['hits']

# --------------------------------- DynamoDB Part ---------------------------------
def get_dynamodb_service(es_response, input_info):
    # Parse the es_response to extract business_id
    business_ids = []
    for i in range(len(es_response)):
        business_ids.append(es_response[i]['_source']['Business_ID'])

    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('yelp-restaurants')
    # Zip code map in Manhattan
    Manhattan_zip_map = {'harlem':[10026, 10027, 10025, 10030, 10037, 10039, 10029, 10035],
                'chelsea':[10001, 10011, 10018, 10019, 10020, 10036],
                'greenwich village':[10012, 10013, 10014],
                'soho':[10012, 10013, 10014],
                'lower manhattan':[10004, 10005, 10006, 10007, 10038, 10280],
                'lower east side':[10002, 10003, 10009],
                'upper east side':[10021, 10028, 10044, 10065, 10075, 10128],
                'upper west side':[10023, 10024, 10025],
                'washington heights':[10031, 10032, 10033, 10034, 10040]
                }
    location = input_info['Messages'][0]['MessageAttributes']['Location']['StringValue'].lower()
    
    recommends = []
    random_index = set()
    # Compile the search results by location
    for id in business_ids:
        item = table.get_item(Key = {"Business_ID": id})
        if int(item['Item']['Zip Code']) in Manhattan_zip_map[location]:
            recommends.append(item)
        if len(recommends) >= 3:
            break

    input_cuisine = input_info['Messages'][0]['MessageAttributes']['Cuisine']['StringValue']

    # Set text message
    start = 'Hello! Here are my ' + input_cuisine + 'restaurants' + ' suggestions' + '.\n'

    recommend_context = ''

    for i in range(len(recommends)):
        recommend_context += '{}. '.format(i+1) + recommends[i]['Item']['Name'] + ', located at ' + recommends[i]['Item']['Address'] + '\n'

    end = 'Enjoy your meal!'
    text_message = start + recommend_context + end

    return text_message


# --------------------------------- SNS Part ---------------------------------
def send_sns_text(input_info, text_message):
    sns = boto3.client("sns", region_name="us-east-1")
    sns.publish(
        PhoneNumber= "1" + input_info['Messages'][0]['MessageAttributes']['PhoneNumber']['StringValue'],
        Message=text_message
    )

# --------------------------------- Lambda Handler Part ---------------------------------
def lambda_handler(event, context):
    input_info = get_sqs_service()
    es_response = get_es_service(input_info)
    text_message = get_dynamodb_service(es_response, input_info)
    send_sns_text(input_info, text_message)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Finished')
    }

