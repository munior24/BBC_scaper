from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List
import os
from bson import ObjectId



app = FastAPI()

# MongoDB connection
client = MongoClient('localhost, 27017')
db = client.bbc
collection = db.articles

# Pydantic models
class Article(BaseModel):
    _id: str
    menu: str
    submenu: str
    topic: str
    text: str
    title: str
    subtitle: str
    date: str
    images: List[str]
    author: str
    videos: List[str]
    url: str

@app.get("/articles/", response_model=List[Article])
def get_articles():
    try:
        articles = list(collection.find())
        for article in articles:
            article['_id'] = str(article['_id'])
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/articles/{article_id}", response_model=Article)
def get_article(article_id: str):
    try:
        article = collection.find_one({"_id": ObjectId(article_id)})
        if article:
            article['_id'] = str(article['_id'])
            return article
        else:
            raise HTTPException(status_code=404, detail="Article not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
