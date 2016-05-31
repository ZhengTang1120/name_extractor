import json

j = json.load(open("webpages.json"))

for page in j["hits"]["hits"]:
	print json.dumps(page)