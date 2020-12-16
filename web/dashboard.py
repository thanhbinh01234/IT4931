# Dashboard
import os
from flask import Flask
from flask import render_template
from pymongo import MongoClient
import json
from bson import json_util
from bson.json_util import dumps

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DBS_NAME = 'twitter'

COLLECTION_NAME2 = 'counts'
COLLECTION_NAME3 = 'ratio'
COLLECTION_NAME4 = 'tracking_word'
COLLECTION_NAME5 = 'users'
COLLECTION_NAME6 = 'time'

FIELDS2 = {'_id': False}
FIELDS3 = {'_id': False}
FIELDS4 = {'_id': False}
FIELDS5 = {'_id': False}
FIELDS6 = {'_id': False}

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/data/counts")
def counts():
    connection = MongoClient(MONGO_URL)
    collection = connection[DBS_NAME][COLLECTION_NAME2]
    projects = collection.find(projection=FIELDS2)
    json_projects = []
    for project in projects:
        json_projects.append(project)
    json_projects = json.dumps(json_projects, default=json_util.default)
    connection.close()
    return json_projects

@app.route("/data/ratio")
def ratio():
    connection = MongoClient(MONGO_URL)
    sentiment_coll = connection[DBS_NAME][COLLECTION_NAME3]
    count_coll = connection[DBS_NAME][COLLECTION_NAME2]
    pos_all = list(sentiment_coll.find(projection=FIELDS3))
    pos = (p['pos'] for p in pos_all)
    total = sum(list(count_coll.find())[:len(pos_all)])
    json_projects = json.dumps([
        {
            'PN': 'P',
            'Ratio': 1 if total == 0 else pos / total
        },
        {
            'PN': 'N',
            'Ratio': 0 if total == 0 else (total - pos) / total
        }
    ])
    # json_projects = []
    # for project in projects:
    #     json_projects.append(project)
    # json_projects = json.dumps(json_projects, default=json_util.default)
    connection.close()
    return json_projects


@app.route("/data/tracking_word")
def tracking_word():
    connection = MongoClient(MONGO_URL)
    collection = connection[DBS_NAME][COLLECTION_NAME4]
    projects = collection.find(projection=FIELDS4)
    json_projects = []
    for project in projects:
        json_projects.append(project)
    json_projects = json.dumps(json_projects, default=json_util.default)
    connection.close()
    return json_projects

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)
