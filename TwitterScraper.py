import snscrape.modules.twitter as sntwitter
from snscrape.base import ScraperException
import datetime as dt
import time
import csv
import os

### FUNCTION CREATION ###

def generateDateList(startYear, startMonth, startDay):
    # Define the starting date
    startDate = dt.datetime(startYear, startMonth, startDay)

    # Define the end date as today
    endDate = dt.datetime.now()

    # Define the time delta of one week
    delta = dt.timedelta(days=1)

    # Generate a list of dates one week apart
    dateList = []
    while startDate <= endDate:
        dateList.append(startDate)
        startDate += delta
    return dateList

def generatePromptList(word, dateList):
    # Creating a prompt list to store all out twitter search terms
    promptList = []

    for j, date in enumerate(dateList):
        # Check if we are at the end of the list or not so that we don't get an index out of bounds error
        if j == len(dateList)-1:
            break
        # Construct the prompt in out desired format
        prompt = word + " since:" + \
            date.strftime("%Y-%m-%d") + " until:" + \
            dateList[j+1].strftime("%Y-%m-%d")
        # Append it to the list
        promptList.append(prompt)

    return promptList

### BEGIN MAIN BODY ###
totalTweets = 0
totalTime = 0

# Create the results folder if it's not already there
if not os.path.exists('results'):
    os.makedirs('results')

#generateDateList("Starting year", "Starting month", "Starting day")
dateList = generateDateList(2020, 1, 1)

#set the number of tweets we want to scrape for each period of time
tweetNum = 100

# Set the keywords we are going to be using
keyWords = ["depressed", "loneliness", "vacation"]
#keyWords = ["stressed", "blissful", "outing", "travel", "snack"]
#keyWords = ["bored", "happy", "sad", "joyful", "food"]

#TODO handle 404 error

for word in keyWords:
    # Generate our prompt list from the date list
    promptList = generatePromptList(word, dateList)

    print(f"Number of prompts:{len(promptList)} Expected number of tweets:{len(promptList) * tweetNum}")

    outputFile = word + ".csv"
    outputFilePath = os.path.join('results', outputFile)

    with open(outputFilePath, 'a', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)

        for prompt in promptList:
            startTime = time.time()

            # Creating list to append tweet data to
            tweetList = []

            tweetScraper = sntwitter.TwitterSearchScraper(prompt).get_items()

            # Using TwitterSearchScraper to scrape data and append tweets to list
            while len(tweetList) < tweetNum:
                try:
                    tweet = next(tweetScraper)

                    tweetList.append([tweet.date, tweet.id, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])
                # Catch any scraper exceptions
                except ScraperException as se:
                    print("Caught ScraperException. Sleeping for 5 min then retrying")
                    time.sleep(60 * 5)
                
            # Flush the tweet list to the file
            for tweet in tweetList:
                writer.writerow(tweet)

            endTime = time.time()
            elapsedTime = endTime - startTime
            totalTime = totalTime + elapsedTime
            totalTweets = totalTweets + tweetNum
            print(f"Pulled {tweetNum} tweets for prompt {prompt} in: {elapsedTime}s")

print(f"\nPulled a total of {totalTweets} in {totalTime/60}m for an average of {totalTweets/totalTime}t/s")