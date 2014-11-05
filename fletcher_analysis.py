#!/usr/bin/env python
# -*- coding: utf-8 -*-

# PROJECT FLETCHER ANALYSES
# Dara Elass
# Logging done with the help of Laurie Skelly

# import modules
import pymongo
from pymongo import MongoClient
import datetime
import HTMLParser
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
import operator
from sklearn.cluster import KMeans, MiniBatchKMeans, AgglomerativeClustering
from collections import defaultdict
from collections import OrderedDict
import numpy as np
from sklearn.metrics import pairwise_distances
from textblob import TextBlob
import os
import sys
from datetime import datetime

# add one global variable (after import statments)
PRINT_LOG_TO_SCREEN_TOO = True

# define functions
def create_logfile_name(helpful_title=None):
    init_time = datetime.now()
    logfile_name = datetime.strftime(init_time,'%Y%m%d_%H%M')
    if helpful_title:
        logfile_name += '_%s' % helpful_title
    return logfile_name
    
def init_log(helpful_title=None):
    global LOG, LOG_NAME
    logfile_name = create_logfile_name(helpful_title)
    LOG_NAME = 'logs/'+logfile_name+'.txt'
    print LOG_NAME
    LOG = open(LOG_NAME,'w')
    return LOG

def to_log(msg):
    if PRINT_LOG_TO_SCREEN_TOO:
        print >> sys.stderr, msg
    LOG.write(msg.encode('utf-8')+"\n")
    
def close_log():
    LOG.close()
    
def check_log():
    with open(LOG_NAME,'r') as logfile:
        for line in logfile.readlines():
            print line

def get_tweets(collection='alltweets'):
    client = MongoClient()
    db = client.projectfletcher
    tweets = db[collection]
    h = HTMLParser.HTMLParser()
    all_tweets = [h.unescape(tweet['text'].lower()) for tweet in tweets.find({},{"text":1,"_id":0})]
    to_log('created all_tweets array with '+str(len(all_tweets))+' tweets')
    return all_tweets

def get_unique_tweets(collection='alltweets'):
    client = MongoClient()
    db = client.projectfletcher
    tweets = db[collection]
    h = HTMLParser.HTMLParser()
    unique_tweets = [h.unescape(tweet['text'].lower()) for tweet in tweets.find({},{"text":1,"_id":0}) if 'RT' not in tweet['text']]
    to_log('created unique tweets array with '+str(len(unique_tweets))+' tweets')
    return unique_tweets

def create_vectorizer(stop_words,n):
    vectorizer =  TfidfVectorizer(ngram_range=(n,n),stop_words=stop_words)
    to_log('create vectorizer')
    return vectorizer

def fit_vectorizer(data, vectorizer):
    X =  vectorizer.fit_transform(data)
    to_log('created X')
    return X

def cluster_minibatchk(k, X, max_iter=200):
    model = MiniBatchKMeans(k, max_iter=max_iter).fit(X)
    to_log('fitted the model')
    clusters = model.predict(X)
    to_log('created clusters')
    return clusters

def create_cluster_dicts(clusters,tweets):
    dict_tweets = defaultdict(list)
    tweets_by_cluster = defaultdict(list)
    for i, cluster in enumerate(clusters):
        dict_tweets[cluster].append(tweets[i])
        tweets_by_cluster[cluster].extend(tweets[i])
    to_log('created dictionaries')
    return dict_tweets

def log_top_tweets_in_cluster(dict_tweets):    
    for cluster_id, tweets in dict_tweets.iteritems():
        msg = '------CLUSTER %s ---------\n' % cluster_id
        for tweet in tweets[:10]:
            msg += tweet
            msg += "\n"
        msg += '--------------------------'
        msg += "\n"
        to_log(msg)

def create_cluster_sizes(k, dict_tweets):
    cluster_sizes = {}
    sorted_cluster_sizes = {}
    for i in range(k):
        num_ppl_in_cluster = len(dict_tweets[i])
        cluster_sizes[i] = num_ppl_in_cluster
        to_log('cluster ' + str(i) + ' has ' + str(cluster_sizes[i]) + ' tweets.')
    return cluster_sizes

def name_the_clusters(k,X,clusters,dict_tweets):
    to_log('calculating centers')
    cluster_centers = MiniBatchKMeans(k).fit(X).cluster_centers_
    min_dist = {}
    cluster_names = []
    for i in range(k):
        X_this_cluster = X[clusters == i]
        cos_dist = np.array(pairwise_distances(X_this_cluster, cluster_centers[i], metric='cosine'))
        id_of_closest = cos_dist.argmin()
        num_ppl_in_cluster = len(dict_tweets[i])
        to_log(str(num_ppl_in_cluster)+ ' people expressed a similar statement to:')
        to_log(dict_tweets[i][id_of_closest])
        cluster_names.append(dict_tweets[i][id_of_closest])
    return cluster_names

def num_nonzero_clusters(cluster_sizes):
    count = 0 #number of nonzero clusters
    for cluster in cluster_sizes:
        if cluster_sizes[cluster] != 0:
            count += 1
    to_log('number of nonzero clusters: '+str(count))
    return count

def get_top_clusters(x,cluster_sizes): # x = how many top clusters? top 10? top 5?
    to_log('top ' +str(x) + ' clusters are:')
    sorted_cluster_sizes = sorted(cluster_sizes.items(), key=operator.itemgetter(1),reverse=True)
    top_clusters = OrderedDict()
    for i in range(x):
        cluster = sorted_cluster_sizes[i][0]
        cluster_size = sorted_cluster_sizes[i][1]
        top_clusters[cluster] = cluster_size
        to_log('cluster number: '+str(cluster)+' with a size of: ' +str(cluster_size))
    return top_clusters

def tweets_to_string(array_of_tweets): # convert array of tweets to one string
    all_text = ""
    for tweet in array_of_tweets:
        all_text += tweet + "\n"
    return all_text

def get_top_words(m,array_of_tweets): # top x words of a group of tweets
    counter = 0
    word_count = defaultdict(int)
    all_text = tweets_to_string(array_of_tweets)
    for word,count in sorted(TextBlob(all_text).word_counts.items(), key=operator.itemgetter(1), reverse=True):
        if word not in stop_words_cluster:
            word_count[word] = count
            counter += 1
            to_log("%15s %i" % (word,count))
        if counter == m:
            break
    return word_count

def get_tfidf_values(m,X_corpus,vectorizer,cluster_id): # n = ngrams, and m is top m words
    vector = X_corpus[cluster_id,]
    feature_names = vectorizer.get_feature_names()
    cluster_tfidf = {}
    for x in X_corpus.nonzero()[1]:
        cluster_tfidf[feature_names[x]] = vector[(0,x)]
    cluster_tfidf_sorted = sorted(cluster_tfidf.items(), key = operator.itemgetter(1), reverse=True)
    for word, tfidf in cluster_tfidf_sorted[:m]:
        to_log('%20s %f' % (word,tfidf))

def calculate_retweets(all_tweets, unique_tweets):
    num_unique_tweets = len(unique_tweets)
    num_all_tweets = len(all_tweets)
    num_retweets = []
    count = 0
    matching = [s for s in all_tweets if "arrests being made after #fergusonoctober shuts down second #walmart of the day. #fergusonoctober #moralmonday" in s]
    for s in matching:
        to_log(s)
    count = len(matching)
    to_log('number of retweets: '+str(count))
    return count
    # for i, tweet in enumerate(unique_tweets):

if __name__ == '__main__':
    # initialize variables and log items
    start_time = datetime.now()
    init_log()
    k = 75 # number of clusters    
    to_log('start time: '+str(start_time))
    to_log('------------------------------')
    to_log('NOTES: testing counting number of retweets')
    to_log('clustering method: minibatchkmeans')
    to_log('k = '+str(k))
    to_log('tokenizing using tfidf vectorizer and the original stopwords')
    to_log('RTs: not included')
    to_log('distance metric: cosine')
    to_log('------------------------------')

    # start analyses
    stop_words_cluster = stopwords.words('english') # create stopwords
    stop_words_cluster.extend(["http","RT","rt","co","ferguson","fergusonoctober"]) # append stopwords
    all_tweets = get_tweets() # get all tweets, RTs included
    unique_tweets = get_unique_tweets() # get only non-RT tweets
    calculate_retweets(all_tweets, unique_tweets)
    vectorizer = create_vectorizer(stop_words_cluster,1) # create vectorizer
    X = fit_vectorizer(unique_tweets, vectorizer) # fit vectorizer
    clusters = cluster_minibatchk(k, X) # create clusters
    dict_tweets = create_cluster_dicts(clusters,unique_tweets) # create dictionary of clustered tweets
    all_tweets_by_cluster = [] # each element is a string of all the tweets together
    for i in range(k):
        all_tweets_by_cluster.append(tweets_to_string(dict_tweets[i]))
    vectorizer_corpus_1gram = create_vectorizer(stop_words_cluster,1) # vectorizer for clusters, 1gram
    X_corpus_1gram = fit_vectorizer(all_tweets_by_cluster, vectorizer_corpus_1gram)
    vectorizer_corpus_2gram = create_vectorizer(stop_words_cluster, 2) # vectorizer for clusters, 2gram
    X_corpus_2gram = fit_vectorizer(all_tweets_by_cluster, vectorizer_corpus_2gram)
    cluster_sizes = create_cluster_sizes(k, dict_tweets) # get cluster sizes
    num_nonzero_clusters(cluster_sizes) # get number of nonzero clusters
    x = 10 # number of clusters to focus on
    m = 15 # number of words/terms to show
    top_clusters = get_top_clusters(x,cluster_sizes) # get top x clusters
    for cluster_num,cluster_size in top_clusters.items(): # for each cluster
        this_cluster_tweets = dict_tweets[cluster_num] # get array of tweets for this cluster
        to_log('word counts for cluster number '+str(cluster_num) + ' with a size: ' + str(cluster_size))
        get_top_words(m,this_cluster_tweets)
        to_log('tfidf values for cluster number '+str(cluster_num) + ' with a size: ' + str(cluster_size)+' ngram = 1')
        get_tfidf_values(m,X_corpus_1gram,vectorizer_corpus_1gram,cluster_num)
        to_log('tfidf values for cluster number '+str(cluster_num) + ' with a size: ' + str(cluster_size)+' ngram = 2')
        get_tfidf_values(m,X_corpus_2gram,vectorizer_corpus_2gram,cluster_num)
        
    # finalize log items
    end_time = datetime.now()
    to_log('end time: '+str(end_time))
    to_log('total time: '+str(end_time-start_time))
    close_log()
