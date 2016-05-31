import pygtrie as trie
import json
import re
from pyspark import SparkContext

def name_extractor(page, t):
    if "name" in page:
        name = page["name"]
    else:
        name = ""
    if "description" in page:
        description = page["description"]
    else:
        description = ""    
    text = name[0]+" "+description

    tokens = text.lower().split(" ")
    temp = set()
    for token in tokens:
        token = re.search('[a-zA-Z].*[a-zA-Z]', token)
        if token:
            value = t.value.get(token.group(0))
            if isinstance(value, basestring):
                temp.add(value)
    return dict(id=page["id"],names=list(temp))

def get_value_json(path, doc, separator='.'):
    paths = path.strip().split(separator)
    for field in paths:
        if field in doc:
            doc = doc[field]
        else:
            return ''

    if type(doc) == dict or type(doc) == list:
        return json.dumps(doc)
    else:
        return doc

def create_input_geonames(line):
    out = {}
    line = json.loads(line)

    fo = get_value_json('_source', line)

    if fo != '':
        json_x = json.loads(fo)

        json_l = []
        if isinstance(json_x, dict):
            json_l.append(json_x)
        elif isinstance(json_x, list):
            json_l = json_x

        for x in json_l:
            # print x
            out['id'] = fo = get_value_json('_id', line)
            out['name'] = get_value_json('name', x)
            out['description'] = get_value_json('description', x)

    return out

if __name__ == "__main__":
    sc = SparkContext(appName="DIG-NameExtraction")

    input_rdd = sc.textFile("webpages.jl")
    input_rdd = input_rdd.map(create_input_geonames)

    t = trie.CharTrie()
    names = json.load(open("names.json"))
    for name in names:
        t[name] = name
    T = sc.broadcast(t)

    results = input_rdd.map(lambda x:name_extractor(x, T))
    results.map(lambda x: json.dumps(x)).saveAsTextFile("out")
