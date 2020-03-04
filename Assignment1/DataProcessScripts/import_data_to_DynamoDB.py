import requests
#import csv
import time
from datetime import datetime
#import ipdb
from decimal import *
#import simplejson as json
import json
import boto3


# define some basic data
API_KEY = 'r9h8syIW55DIppb1IfV4ujRBXdVW_pzTptZ5sk1kRbJvX-yLeoSqaWEqPXKE6wIBYMEfNa1VdE65UVjJKhkky5TzCWxvnrHRQdU5MREUCuqEpDkzRDGzrnje_MBMXnYx'
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
URL = "https://api.yelp.com/v3/businesses/search"
HEADERS = {'Authorization': 'bearer %s' % API_KEY}
URL_PARAMETERS = {'term': 'food', 
                  'limit': 50,
                  'radius': 15000,
                  'offset': 200,
                  'location': 'Manhattan'}

cuisine_types = ['chinese', 'korean', 'india', 'japanese', 'vietnamese', 'american', 'italian', 'french', 'thai', 'mexican', 'mediterranean', 'turkish', 'greek', 'germany', 'malaysia', 'brunch', 'steak', 'seafood', 'bbq', 'brunch', 'dessert', 'pizza', 'bar', 'cafe']

manhattan_locations = ['Lower East Side, Manhattan',
                       'Upper East Side, Manhattan',
                       'Upper West Side, Manhattan',
                       'Washington Heights, Manhattan',
                       'Central Harlem, Manhattan',
                       'Chelsea, Manhattan',
                       'Manhattan',
                       'East Harlem, Manhattan',
                       'Gramercy Park, Manhattan',
                       'Greenwich, Manhattan',
                       'Lower Manhattan, Manhattan']

# check the item is empty or not
def check_empty(info):
    if len(str(info)) == 0:
        return 'N/A'
    else:
        return info


dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('yelp-restaurants')


#start_time = time.time()
for location in manhattan_locations:
    URL_PARAMETERS['location'] = location
    for cuisine in cuisine_types:
        URL_PARAMETERS['term'] = cuisine

        # request to yelp api
        response = requests.get(url = URL, params = URL_PARAMETERS, headers = HEADERS)
        # parse the return data
        business_data = response.json()['businesses']
        for business in business_data:
            # create key - insertedAtTimestamp
            current_time = datetime.now()
            time_str = current_time.strftime("%d/%m/%Y %H:%M:%S")
            # put the data into dynamodb
            table.put_item(
            Item = {
                'Business_ID':check_empty(business['id']),
                'insertedAtTimestamp': check_empty(time_str),
                'Name':  check_empty(business['name']),
                'Cuisine': check_empty(cuisine),
                'Rating': check_empty(Decimal(business['rating'])),
                'Number of Reviews' : check_empty(Decimal(business['review_count'])),
                'Address': check_empty(business['location']['address1']),
                'Zip Code': check_empty(business['location']['zip_code']),
                'Latitude': check_empty(str(business['coordinates']['latitude'])),
                'Longitude': check_empty(str(business['coordinates']['longitude']))
            }
            )














