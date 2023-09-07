import itertools
from pymongo import MongoClient
import spacy
import json
from spacy.tokens import DocBin
from bson.objectid import ObjectId
from tqdm import tqdm


client = MongoClient("mongodb://localhost:27017")

db = client['test']

ents = db['entities']
errors = db['errors']

nlp = spacy.blank("en")

def update_entity(doc, update):
    ents.update_one({"_id": ObjectId(doc['_id'])},{"$set": update })
    
def save_error(error, id):
    errors.insert_one({"error":error, "ent_id":id})

for ent in tqdm(ents.find({"docbin":{"$exists":False}})):
    if "docbin" in ent:
        continue
    db = DocBin()
    if "text" in ent:
        text = ent["text"]
    else:
        save_error("text not found",ent["_id"])
        continue
    if "entities" in ent:    
        entlist = ent["entities"]
    else:
        save_error("entities not found",ent["_id"])
        continue

    if len(entlist)==0: #means no entities marked
        update_entity(ent, {"docbin":False})
        continue

    entlist.sort()
    unique_entities = [ent for ent,_ in itertools.groupby(entlist) ]
    doc = nlp.make_doc(text)
    valid_ents = []
    try:
        for start, end, label in unique_entities:
            span = doc.char_span(start,end,label=label,alignment_mode="contract")
            
            if span is None or span.text.startswith(" ") or span.text.endswith(" ") :
                save_error(" Skipping Entity for span is None", ent["_id"])
                update_entity(ent, {"docbin":False})
                continue
            else:
                update_entity(ent, {"docbin":True})
                valid_ents.append(span)
        doc.ents = valid_ents
        db.add(doc)
    except Exception as ex:
        save_error(str(ex), ent["_id"])
        update_entity(ent, {"docbin":False})



    # exit()