import json
from bson import json_util


from IPython.display import clear_output
from datetime import datetime
import pytz
import time
from sseclient import SSEClient as EventSource
from ksql import KSQLAPI


def construct_event(event_data, user_types):
    # init dictionary of namespaces
    namespace_dict = init_namespaces()
    
    # use dictionary to change assign namespace value and catch any unknown namespaces (like ns 104)
    try:
        event_data['namespace'] = namespace_dict[event_data['namespace']]
    except KeyError:
        event_data['namespace'] = 'unknown'

    # assign user type value to either bot or human
    user_type = user_types[event_data['bot']]

    # define the structure of the json event that will be published to kafka topic
    
    event = [{"domain": event_data['meta']['domain'],
        "namespaceType": event_data['namespace'],
        "title": event_data['title'],
        #"comment": event_data['comment'],
        "timestamp": event_data['meta']['dt'],
        "userName": event_data['user'],
        "userType": user_type,
        #"minor": event_data['minor'],
        "oldLength": event_data['length']['old'],
        "newLength": event_data['length']['new']
    }]
    
    return event


def init_namespaces():
    # create a dictionary for the various known namespaces
    # more info https://en.wikipedia.org/wiki/Wikipedia:Namespace#Programming
    namespace_dict = {-2: 'Media', 
                      -1: 'Special', 
                      0: 'main namespace', 
                      1: 'Talk', 
                      2: 'User', 3: 'User Talk',
                      4: 'Wikipedia', 5: 'Wikipedia Talk', 
                      6: 'File', 7: 'File Talk',
                      8: 'MediaWiki', 9: 'MediaWiki Talk', 
                      10: 'Template', 11: 'Template Talk', 
                      12: 'Help', 13: 'Help Talk', 
                      14: 'Category', 15: 'Category Talk', 
                      100: 'Portal', 101: 'Portal Talk',
                      108: 'Book', 109: 'Book Talk', 
                      118: 'Draft', 119: 'Draft Talk', 
                      446: 'Education Program', 447: 'Education Program Talk', 
                      710: 'TimedText', 711: 'TimedText Talk', 
                      828: 'Module', 829: 'Module Talk', 
                      2300: 'Gadget', 2301: 'Gadget Talk', 
                      2302: 'Gadget definition', 2303: 'Gadget definition Talk'}

    return namespace_dict


client = KSQLAPI('http://localhost:8088')

# used to parse user type
user_types = {True: 'bot', False: 'human'}

# consume websocket
url = 'https://stream.wikimedia.org/v2/stream/recentchange'
   
for event in EventSource(url):
    if event.event == 'message':
        try:
            event_data = json.loads(event.data)
        except ValueError:
            pass
        else:
            # filter out events, keep only article edits (mediawiki.recentchange stream)
            if event_data['type'] == 'edit':
                # construct valid json event
                event_to_send = construct_event(event_data, user_types)

                results = client.inserts_stream("Wikipedia_STREAM", event_to_send)
      
                print(results)
                print(event_to_send)
                time.sleep(2)
