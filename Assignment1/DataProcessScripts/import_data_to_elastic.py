import json
import os

# Load json data
file = open("yelp-restaurants-data.json")
file = json.load(file)

# Write data to ElasticSearch
for index, item in enumerate(file['Items']):
    part1 = 'curl -XPUT https://search-restaurants-e3eoppeoaezsmv7mtfavlopz5e.us-east-2.es.amazonaws.com/restaurants/Restaurant/{} -d'.format(index)
    # Extract part of the information from the whole data
    content = "'"+ "{" + '"Business_ID": "{}", "Cuisine": "{}"'.format(item['Business_ID']['S'],item['Cuisine']['S'] ) + "}" +"' "
    part2 = "-H " + "'" + "Content-Type: application/json" + "'"
    # Combine the command together
    full_command = part1 + content + part2
    # Carry out the command
    os.system(full_command)
