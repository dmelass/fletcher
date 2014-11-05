<b>PROJECT FLETCHER</b>

In this project, I gather tweets using Twitter's API in Python, and cluster them using MiniBatchKMeans.

<b>In this respository...</b>

tweet_collection.py - this script uses tweepy to use the API and pymongo to store the tweets and their information on a mongo collection.

fletcher_analysis.py - this script uses MiniBatchKMeans from the sklearn module to create clusters.

I wrote a <b>blog post</b> summarizing this project [here](https://dmelass.github.io/blog/2014/11/04/clustering-ferguson/).