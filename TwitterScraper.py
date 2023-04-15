import snscrape.modules.twitter as sntwitter
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

def generatePromptList(phrases, dateList):
    # Creating a prompt list to store all out twitter search terms
    promptList = []

    for word in phrases:
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
dateList = generateDateList(2013, 1, 1)

#set the number of tweets we want to scrape for each period of time
tweetNum = 100

# Set the keywords we are going to be using
keyWords = ["vacation", "depressed", "stressed", "blissful", "outing", "travel", "board"]
#keyWords = ["alone", "loneliness", "happy", "sad", "joyful", "food", "snack"]

# Generate our prompt list from the date list
promptList = generatePromptList(keyWords, dateList)

# Creating list to append tweet data to
tweetList = []

print(f"Number of prompts:{len(promptList)} Expected number of tweets:{len(promptList) * tweetNum}")

for j in range(len(promptList)):
    startTime = time.time()

    phrase = promptList[j].split('since')[0].strip()
    outputFile = str(phrase) + ".csv"
    outputFilePath = os.path.join('results', outputFile)

    # Using TwitterSearchScraper to scrape data and append tweets to list
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(promptList[j]).get_items()):
        if i == tweetNum:
            break
        tweetList.append([tweet.date, tweet.id, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])

    with open(outputFilePath, 'a', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)

        for tweet in tweetList:
            writer.writerow(tweet)

    endTime = time.time()
    elapsedTime = endTime - startTime
    totalTime = totalTime + elapsedTime
    totalTweets = totalTweets + tweetNum
    print(f"Pulled {tweetNum} tweets for prompt {promptList[j]} in: {elapsedTime}s")

print(f"\nPulled a total of {totalTweets} in {totalTime/60}m for an average of {totalTweets/totalTime}t/s")