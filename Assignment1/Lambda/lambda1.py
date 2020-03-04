import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

sqs = boto3.client('sqs')
queue_url = 'https://us-east-1.amazonaws.com/135675965852/UserInfo'


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }
    
def elicit_intent(session_attributes, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitIntent',
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


""" --- Helper Functions --- """


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False
        
def isvalid_people(people):
    try :
        num = int(people) 
        if num > 0:
            return True;
        else:
            return False;
        
    except ValueError:
        return float('nan')



def validate_order_restaurants(location, cuisine_type, date, time, number_of_people, phone_number):
    restaurant_locs = ['harlem','chelsea','greenwich village','soho','lower manhattan','lower east hide','upper east side','upper west side','washington heights']
    if location is not None and location.lower() not in restaurant_locs:
        return build_validation_result(False,
                                       'DinnerLocation',
                                       'We have no service on {}, would you like a different place? Our most popular place for dinner is Manhattan'.format(location))
    
    restaurant_types = ['italian', 'chinese', 'mexican', 'american', 'japanese', 'pizza', 'healthy', 'brunch', 'korean', 'thai', 'vietnamese', 'indian', 'seafood', 'dessert']
    if cuisine_type is not None and cuisine_type.lower() not in restaurant_types:
        return build_validation_result(False,
                                       'DinnerType',
                                       'We do not have {}, would you like a different type?  '
                                       'Our most popular type is Chinese food'.format(cuisine_type))
                                       
    if number_of_people is not None:
        if not isvalid_people(number_of_people):
            return build_validation_result(False, 'DinnerPeople', 'I did not understand that, how many people are in your party?')
    
    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(False, 'DinnerDate', 'I did not understand that, what date would you like to dinner?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() <= datetime.date.today():
            return build_validation_result(False, 'DinnerDate', 'You can choose date from tomorrow onwards.  What day would you like to dinner?')
            
    
    if time is not None:
        
        if len(time) != 5:
            return build_validation_result(False, 'DinnerTime', "Please input a valid time like 12:00.")
        for i in range(len(time)):
            if i == 2:
                if time[i] != ":":
                    return build_validation_result(False, 'DinnerTime', "Please input a valid time like 12:00.")
            else:
                if not time[i].isalnum():
                    return build_validation_result(False, 'DinnerTime', "Please input a valid time like 12:00.")

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'dinning_time', None)

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DinnerTime', )
            
    return build_validation_result(True, None, None)

""" --- Functions that control the bot's behavior --- """

def thanking(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Happy to help. Have a great day!'
        }
    )
    
def greeting(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return elicit_intent(
        session_attributes,
        {
            'contentType': 'PlainText',
            'content': 'How can I be of assistance to you today?'
        }
    )

def order_restaurant(intent_request):

    location = get_slots(intent_request)["DinnerLocation"]
    cuisine_type = get_slots(intent_request)["DinnerType"]
    date = get_slots(intent_request)["DinnerDate"]
    time = get_slots(intent_request)["DinnerTime"]
    
    number_of_people = get_slots(intent_request)["DinnerPeople"]
    phone_number = get_slots(intent_request)["DinnerPhone"]
    
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':

        slots = get_slots(intent_request)

        validation_result = validate_order_restaurants(location, cuisine_type, date, time, number_of_people, phone_number)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
        return delegate(output_session_attributes, get_slots(intent_request))
    
    slots = get_slots(intent_request)   
    send_sqs(slots)
    
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you! I have collected a few trendy options for you, I will sent detailed information to your phone:{}'.format(phone_number)})

def send_sqs(slots):
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=1,
        MessageAttributes={
            'Cuisine': {
                'DataType': 'String',
                'StringValue': slots['DinnerType']
            },
            'Location': {
                'DataType': 'String',
                'StringValue': slots['DinnerLocation']
            },
            'PhoneNumber': {
                'DataType': 'String',
                'StringValue': slots['DinnerPhone']
            }
        },
        MessageBody=(
            'Restauraunt Request Information'
        )
    )

""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DinnerOrder':
        return order_restaurant(intent_request)
    if intent_name == 'Welcome':
        return greeting(intent_request)
    if intent_name == 'Thank':
        return thanking(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)

