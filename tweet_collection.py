# This script was written with the help of Irmak Sirer.

import requests
from requests_oauthlib import OAuth1
from pprint import pprint
import tweepy
import pymongo
from tweepy.utils import import_simplejson
from pymongo import MongoClient
import time, datetime
import sys
import dateutil.parser

client = MongoClient()
db = client.projectfletcher # create database
twitter_data = db.twitterdata_with_retweets # create collection

consumer_key = "KRjGyro3hXwdcpxPmBmDwRSMR"
consumer_secret = "bBShkHPyqx1fDnhEDFleuQuuCxi1WnTZIMhAKxYg8NJPFbFdaJ"
access_token = "2834111842-CkpQVeYGS40xBjBZj1iOILZ6MSQPFjIZs3Tcm6k"
access_secret = "hEdPWbweLjFArC3qG32f3YVCx7EIUR223UQpClrT1fzzY"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

oauth = OAuth1(consumer_key,
                client_secret=consumer_secret,
                resource_owner_key=access_token,
                resource_owner_secret=access_secret)

api = tweepy.API(auth)

def tweet_to_doc(tweet):
    doc = {}
    doc["text"] = tweet.text
    doc["created_at"] = tweet.created_at
    doc["entities"] = tweet.entities
    doc["favorite_count"] = tweet.favorite_count
    doc["id"] = tweet.id
    doc["lang"] = tweet.lang
    if tweet.place:
        doc["place"] = tweet.place.full_name
    else:
        doc["place"] = None
    doc["user_name"] = tweet.user.name
    doc["user_followers"] = tweet.user.followers_count
    doc["user_friends"] = tweet.user.friends_count
    doc["user_fav_count"] = tweet.user.favourites_count
    doc["user_desc"] = tweet.user.description
    doc["user_created_at"] = tweet.user.created_at
    return doc

def dict_to_doc(tweet):
    doc = {}
    doc["text"] = tweet['text']
    doc["created_at"] = dateutil.parser.parse(tweet['created_at'])
    doc["entities"] = tweet['entities']
    doc["favorite_count"] = tweet['favorite_count']
    doc["id"] = tweet['id']
    doc["lang"] = tweet['lang']
    doc["retweet_count"] = tweet['retweet_count']
    if tweet['place']:
        doc["place"] = tweet['place']['full_name']
    else:
        doc["place"] = None
    doc["user_name"] = tweet['user']['name']
    doc["user_followers"] = tweet['user']['followers_count']
    doc["user_friends"] = tweet['user']['friends_count']
    doc["user_fav_count"] = tweet['user']['favourites_count']
    doc["user_desc"] = tweet['user']['description']
    doc["user_created_at"] = tweet['user']['created_at']
    return doc

step_size = int(7e+13)
query = '#fergusonoctober OR #ferguson'

with open("bookmark.dat",'r') as bookmarkfile:
    last_id = int(bookmarkfile.read())
get_up_to = last_id + step_size
        
while True:
    last_time = twitter_data.aggregate( { "$group": {"_id": "",
                                        "last_time": {"$max": "$created_at"}}})['result'][0]['last_time']
    hours_since_last_tweet = (datetime.datetime.now() - last_time).total_seconds()/3600.
    if hours_since_last_tweet < 4:
        # WE ARE DONE. NO MORE HOPS
        break

    # HOP START
    print 'last id:', last_id
    print 'get up to:', get_up_to
    hop_batch_counter = 0
    # HOP
    while True:
        with open("bookmark.dat",'r') as bookmarkfile:
            last_id = int(bookmarkfile.read())
        with open("getupto.dat",'r') as getupfile:
            get_up_to = int(getupfile.read())
        end_of_hop = False
        print 'Making request with params: since:%s  up_to:%s' % (last_id,get_up_to)
        parameters = {"q": query, "since_id": last_id, "max_id":get_up_to, "count":100}
        response = requests.get("https://api.twitter.com/1.1/search/tweets.json",
                                params = parameters,
                                auth=oauth)
        if response.status_code != 200:
            print response.status_code, 'request failed, waiting 15 mins'
            print '* time is', datetime.datetime.now()
            print '** last date in db is', last_time
            print '*** earliest date in hop is', twitter_data.find({"id": {"$lt":get_up_to}},{"created_at":1}).sort("created_at",-1).limit(1)[0]['created_at']
            print '****length of collection is', twitter_data.count()
            time.sleep(900)
            continue
        tweets_list = response.json()['statuses']
        
        # FIRST STEP OF A HOP
        if tweets_list == [] and hop_batch_counter == 0:
            # first step of a hop too small
            print 'First step of hop too small, trying a bigger jump.'
            get_up_to += step_size
            with open("getupto.dat",'w') as getupfile:
                getupfile.write(str(get_up_to))
            continue
        # END OF HOP CONDITION
        if len(tweets_list) <100 and hop_batch_counter > 0:
            end_of_hop = True
        # REGULAR HOP STEP: INSERT TWEETS
        hop_batch_counter += 1
        print '***** This is request number %i in this hop' % hop_batch_counter
        if len(tweets_list) == 0:
            print 'No tweets found in the last request (end of hop).'
        else:
            docs_list = map(dict_to_doc, tweets_list)
            min_id_in_hop = min(doc['id'] for doc in docs_list)
            print len(docs_list), " tweets in this batch"
            duplicate_counter = 0
            inserted_counter = 0
            for doc in docs_list:
                try:
                    if twitter_data.find({"id":doc['id']},{"id":1}).limit(1).count() == 1:
                        print 'Duplicate warning. twitter id:', doc['id']
                        duplicate_counter += 1
                    else:
                        twitter_data.insert(doc)
                        inserted_counter += 1
                except:
                    print 'Warning: Error for twitter id: %s' % doc['id']
            print '%i tweets inserted.' % inserted_counter
            print '%i duplicates in this batch.' % duplicate_counter
        
        # END OF HOP
        if end_of_hop:
            # record the new bookmark (our last_id)
            print "-------------END OF HOP-----------"
            last_id = twitter_data.aggregate( { "$group": {"_id": "",
                                                           "last_id": {"$max": "$id"}}})['result'][0]['last_id']
            with open("bookmark.dat",'w') as bookmarkfile:
                bookmarkfile.write(str(last_id))
            # end the hop loop
            get_up_to = last_id + step_size
            with open("getupto.dat",'w') as getupfile:
                getupfile.write(str(get_up_to))
            break
        else:
            # keep going with the next batch in the HOP
            get_up_to = min_id_in_hop
            with open("getupto.dat",'w') as getupfile:
                getupfile.write(str(get_up_to))
