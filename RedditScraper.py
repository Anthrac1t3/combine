import praw
import pandas as pd
#import requests
import json

#Open the json file, load it's contents into a JSON object
loginFile = open('login.json')
loginCredentials = json.load(loginFile)

#Close the file for good measure
loginFile.close()

#Load the credentials from the JSON object into our variables
#username = loginCredentials['username']
userAgent = loginCredentials['userAgent']
#password = loginCredentials['password']
clientSecret = loginCredentials['clientSecret']
clientId = loginCredentials['clientId']

#Open the json file, load it's contents into a JSON object
configFile = open('config.json')
settings = json.load(configFile)

#Close the file for good measure
configFile.close()

#Load the list of subreddits we want to scrape into a list
subreddits = settings['subredditsList']

#Create the PRAW Reddits instance with all of our credentials. This instance is a read-only instance
reddit = praw.Reddit(client_id=clientId, client_secret=clientSecret, user_agent=userAgent)

postsDictionary = {"Text": []}

#Iterate through the list of subreddits we have and pull data from each
for subredditName in subreddits:
    print("Processing: ", subredditName)
    subreddit = reddit.subreddit(subredditName)

    #Process all of the hundred top posts
    for post in subreddit.top(limit=10):
        entirePostText = post.title
        entirePostText.append(post.selftext)
        postsDictionary["Title"].append(entirePostText)
        # Text inside a post
        #postsDictionary["Post Text"].append(post.selftext)
        # Unique ID of each post
        #postsDictionary["ID"].append(post.id)
        # The score of a post
        #postsDictionary["Score"].append(post.score)
        # Total number of comments inside the post
        #postsDictionary["Total Comments"].append(post.num_comments)
        # URL of each post
        #postsDictionary["Post URL"].append(post.url)

    # Saving the data in a pandas dataframe
    topPosts = pd.DataFrame(postsDictionary)
    topPosts.to_csv("posts.csv", index=True)