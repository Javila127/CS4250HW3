#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #3
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient
import datetime
import string

def connectDataBase():

    # Creating a database connection object using pymongo

    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # removing punctuation from text
    translator = str.maketrans('', '', string.punctuation)
    clean_text = docText.translate(translator)

    num_chars = sum(1 for char in clean_text if char not in string.whitespace)

    # Produce a final document as a dictionary including all the required document fields
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "num_chars": num_chars,
        "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "category": docCat,
        "terms": []
    }

    # Split text into terms
    terms = clean_text.lower().split()
    term_counts = {}
    for term in terms:
        term_counts[term] = term_counts.get(term, 0) + 1

    # Create term objects and update document terms list
    for term, count in term_counts.items():
        document["terms"].append({
            "term": term,
            "num_chars": len(term),
            "term_count": count
        })

    # Insert the document
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count
    index = {}
    cursor = col.find({}, {"terms": 1, "title": 1})
    for doc in cursor:
        for term_info in doc.get("terms", []):
            term = term_info.get("term")
            count = term_info.get("term_count")
            if term is not None and count is not None:
                if term not in index:
                    index[term] = []
                index[term].append(f"{doc['title']}: {count}")
    return index
