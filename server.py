from bson import ObjectId
from flask import Flask, render_template
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client['test']
ents = db['entities']
errors = db['errors']
app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/td/<id>")
def getTD(id):
    ent = ents.find_one({"_id":ObjectId(id)},{"_id":0})
    return ent



if __name__ == "__main__":
    app.run(debug=True, port=8080)