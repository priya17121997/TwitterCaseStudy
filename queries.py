from flask import Flask, jsonify
from pymongo import MongoClient
import json
from bson import json_util
from nltk.corpus import stopwords


app = Flask(__name__)
app.config["DEBUG"] = True

client = MongoClient("mongodb://localhost:27017")  # host uri

db=client.TwitterTestDB
tasks_collection = db.New_Twitter_Tweets


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        return json.JSONEncoder.default(self, obj)


#@app.route('/api/get_tasks11', methods=['GET'])
#def query11():
  #  ans = tasks_collection.count()

   # return jsonify(total_count = ans)



@app.route('/api/query11', methods=['GET'])
def query1():
    cursor = tasks_collection.aggregate([
        { '$group' :
              { '_id' : '$location',
                'Count' : {'$sum' : 1}
                }
          }
    ])

    #enc = CustomEncoder()
    ans = []
    for doc in cursor:
        ans.append(doc)

    return jsonify(ans)

@app.route('/api/query2', methods=['GET'])
def query2():
    cursor = tasks_collection.aggregate([
        {'$group':
             {'_id': {'location': '$location','date': '$date'},
              'Count': {'$sum': 1}
              }
         },
        { "$sort" : { "_id.location" : 1, "Count" : -1}}
    ])
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)


@app.route('/api/query3', methods=['GET'])
def query3():
    cursor =  tasks_collection.aggregate([
        { "$project" : { "tweets" : { "$split" : ["$tweet", " "] } } },
        { "$unwind" : "$tweets" },
        { "$match" : {"tweets" :
                          { "$nin" : ["the","is","are", "am", "will", "shall", "on","in","to","a","and","for","have","just","we","or","you","not","me","my","of","","as","but", "at","so","all","was"]}}},
        { "$group" : { "_id" :  "$tweets"  , "total" : { "$sum" : 1 } } },
        { "$sort"  : { "total" : -1 } } ,
        {"$limit" : 100}
    ])
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)

@app.route('/api/query4', methods=['GET'])
def query4():
    en_stops = list(set(stopwords.words('english')))
    cursor = tasks_collection.aggregate([
        { "$project" : {"location": "$location" , "word" : { "$split" : ["$tweet", " "] }}},
        { "$unwind" : "$word"},
        {'$project': {'word': {'$toLower': "$word"}, 'location': 1}},
        {'$match': {
            '$and': [
                {'word': {'$ne': ""}},
                {'word': {'$nin': en_stops}}
            ]}},
        { "$group" : { "_id": {"location": "$location","word":  "$word"} , "total" : { "$sum" : 1 } } },
        { "$sort" : { "total" :-1}}  ,
        { "$group" : { "_id" : "$_id.location" , "Top_Words" : { "$push" : {"word" : "$_id.word", "total" : "$total"}}}},
        { "$project" : {"location" :1 , "top100Words" : { "$slice" : ["$Top_Words" ,100]}}}
    ], allowDiskUse = True)
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)



# db.Tweets.createIndex( { tweet: "text" } );
@app.route('/api/query5', methods=['GET'])
def query5():
    """Query to find Top 10 preventive / precautionary measures suggested by WHO worldwide /country wise

    :returns: A json representation of array of documents(ans) which is passed an argument.
    """
    cursor = tasks_collection.find(
        {"$text": {"$search": "WHO prevent measure precaution prevention preventive measures", "$caseSensitive": False}},
        # {"$match": {"$text": {"$search": "\"WHO\" \"precaution\" \"preventive\"", "$caseSensitive": True}}},
        # {
        #     '$and': [
        #         {"$text": {"$search": "WHO"}},
        #         {"$text": {"$search": "prevent measure care precaution prevention preventive measures"}}]},
        {"tweet": 1, "_id": 0}).limit(10)
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)

@app.route('/api/query6', methods=['GET'])
def query6():
    """Query to find the total no. of donations received towards COVID-19 country wise, in all the affected countries

    :returns: A json representation of array of documents(ans) which is passed an argument.
    """
    cursor = tasks_collection.aggregate(
        [{"$match": {"$text": {"$search": "donations"}}},
         {"$group": {"_id": "$location", "count": {"$sum": 1}}}])
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)




@app.route('/api/query7', methods=['GET'])
def query7():
    """Query to find the ranking of impacted countries over the last 2 month on a week basis, to see who was standing
    where at any given week

    :returns: A json representation of array of documents(ans) which is passed an argument.
    """
    cursor = tasks_collection.aggregate([
        {"$match": {"$text": {"$search": "cases"}}},
        # {pipeline to extract the number right before cases or after or anywhere in the tweet and convert to integer
        # and store it in cases variable or any other idea},


        {"$group": {"_id": { "location" : "$location", "date" : "$date"}}}

        # {"sort": {"cases": -1}}
    ])
    ans = []
    for doc in cursor:
        ans.append(doc)
    return jsonify(ans)



# @app.route('/api/sum', methods=['POST'])
# def get_tasks1():
#     req_data = request.get_json()
#     ans=req_data[0]["val1"]+req_data[0]["val2"]
#
#     return jsonify(sum = ans)
#[{val1:1,val2:2}]

app.run()
