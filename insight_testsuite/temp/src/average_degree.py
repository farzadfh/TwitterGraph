# -*- coding: utf-8 -*-
"""
Created on Sun Apr  3 17:30:51 2016

@author: farzad
"""

#%%

from __future__ import division
import json
import time
import pandas as pd
import bisect
from pandas import DataFrame
import sys
#sys.stdout = open('tmp.txt', 'w')


if len(sys.argv) == 3:
    tweets_in_filename = sys.argv[1]
    tweets_ou_filename = sys.argv[2]
else:
    tweets_in_filename = '../tweet_input/tweets.txt'
    tweets_ou_filename = '../tweet_output/output.txt'

print(tweets_in_filename)
    
tweets_in_file = open(tweets_in_filename,'r')
tweets_ou_file = open(tweets_ou_filename,'w')

#%%
start_time = time.mktime(time.strptime('Mar 20 00:00:00 +0000 2006','%b %d %H:%M:%S +0000 %Y'))
window_length = 60
def parse_next_tweet():
    global tweets_in_file,start_time
    created_at = 0
    hashtags = ['']
    s = tweets_in_file.readline()
    if s == '':
        status = 'end_of_file'
        return status, created_at, hashtags
    
    s_decoded = json.loads(s)
    if not ('created_at' in s_decoded):
        status = 'not_a_tweet'
        return status, created_at, hashtags
        
    created_at = s_decoded['created_at']
    created_at = time.mktime(time.strptime(created_at,'%a %b %d %H:%M:%S +0000 %Y'))-start_time
    hashtags = s_decoded['entities']['hashtags']
    hashtags = [x['text'] for x in hashtags]
    hashtags = list(set(hashtags))
    
    if len(hashtags)<2:
        status = '<2_hashtags'
        return status, created_at, hashtags
    else:
        status = '>=2_hashtags'
        return status,created_at,hashtags
        
def add_new_tweet(created_at,hashtags):
    global recent_tweets, edges
    j = bisect.bisect(recent_tweets['created_at'].values,created_at)
    a = recent_tweets.iloc[0:j]
    b = DataFrame({'created_at':created_at, 'hashtags':['']}, index=[0])
    c = recent_tweets.iloc[j:]
    recent_tweets = pd.concat([a,b,c], ignore_index=True)
    #recent_tweets.set_value(j,'hashtags',hashtags)
    recent_tweets['hashtags'].iloc[j]=hashtags
    for i in range(len(hashtags)-1):
        for j in range(i+1,len(hashtags)):
            node1 = min(hashtags[i],hashtags[j])
            node2 = max(hashtags[i],hashtags[j])
            edge = (node1,node2)
            if edge in edges:
                edges[edge] = max(edges[edge],created_at)
            else:
                edges[edge] = created_at
                if node1 in vertices:
                    vertices[node1] += 1
                else:
                    vertices[node1]  = 1
                if node2 in vertices:
                    vertices[node2] += 1
                else:
                    vertices[node2]  = 1
                
def remove_old_tweets(latest_tweet_time):
    global recent_tweets,window_length,edges
    all_are_old_tweets = 1
    i = 0
    for i in range(len(recent_tweets)):
        if recent_tweets['created_at'].iloc[i] > latest_tweet_time - window_length:
            all_are_old_tweets = 0
            break
    n_old_tweets = i+all_are_old_tweets
    if len(recent_tweets)>0:
        for k in range(n_old_tweets):
            hashtags = recent_tweets['hashtags'].iloc[k]
            for i in range(len(hashtags)-1):
                for j in range(i+1,len(hashtags)):
                    node1 = min(hashtags[i],hashtags[j])
                    node2 = max(hashtags[i],hashtags[j])
                    edge = (node1,node2)
                    if edge in edges and edges[edge] <= latest_tweet_time - window_length:
                        del edges[edge]
                        vertices[node1] -= 1
                        vertices[node2] -= 1
                        if vertices[node1] == 0:
                            del vertices[node1]
                        if vertices[node2] == 0:
                            del vertices[node2]
    recent_tweets = recent_tweets.iloc[n_old_tweets:]
#%%
tweets_in_file.seek(0)
#recent_tweets = DataFrame({'created_at':0,'hashtags':['']},index=[0])
latest_tweet_time = 0
recent_tweets = DataFrame(columns = ['created_at','hashtags'])
edges = dict({})
vertices = dict({})
status = ''
while status != 'end_of_file':
    status,created_at,hashtags = parse_next_tweet()
    if status == '>=2_hashtags' or status == '<2_hashtags':
        changed = 0
        if status == '>=2_hashtags' and created_at > latest_tweet_time - window_length:
            add_new_tweet(created_at,hashtags)
            changed = 1
            
        if latest_tweet_time < created_at:
            latest_tweet_time = created_at
            remove_old_tweets(latest_tweet_time)     
            changed = 1
        
        if len(vertices)==0:
            avg_deg = 0
        else:
            avg_deg = 2*len(edges)/len(vertices)
        tweets_ou_file.write('%0.2f\n' %avg_deg)
        if 0:            
            print('\n--\n')
            print(latest_tweet_time)
            print('\n')      
            print(recent_tweets)
            print('\n')
            print(edges)
            print('\n')
            print(vertices)
            
tweets_in_file.close()
tweets_ou_file.close()