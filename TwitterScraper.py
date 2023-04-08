import pandas as pd
import snscrape.modules.twitter as sntwitter
import datetime as dt
import time

### FUNCTION CREATION ###

def generateDateList(startYear, startMonth, startDay):
    startTime = time.time()
    # Define the starting date
    startDate = dt.datetime(startYear, startMonth, startDay)

    # Define the end date as today
    endDate = dt.datetime.now()

    # Define the time delta of one week
    delta = dt.timedelta(weeks=1)

    # Generate a list of dates one week apart
    dateList = []
    while startDate <= endDate:
        dateList.append(startDate)
        startDate += delta
    endTime = time.time()
    elapsedTime = endTime - startTime
    print(f"Generated date list in: {elapsedTime}s")
    return dateList

def generatePromptList(dateList):
    startTime = time.time()
    #Creating a prompt list to store all out twitter search terms
    promptList = []

    for i, date in enumerate(dateList):
        #Check if we are at the end of the list or not so that we don't get an index out of bounds error
        if i == len(dateList)-1:
            break
        #Construct the prompt in out desired format
        prompt = "food since:" + date.strftime("%Y-%m-%d") + " until:" + dateList[i+1].strftime("%Y-%m-%d")
        #Append it to the list
        promptList.append(prompt)
    endTime = time.time()
    elapsedTime = endTime - startTime
    print(f"Generated prompt list in: {elapsedTime}s")
    return promptList

### BEGIN MAIN BODY ###
totalTweets = 0
totalTime = 0

#generateDateList("Starting year", "Starting month", "Starting day")
dateList = generateDateList(2023, 1, 1)

#set the number of tweets we want to scrape for each period of time
tweetNum = 10

# Generate our prompt list from the date list
promptList = generatePromptList(dateList)

# Creating list to append tweet data to
tweets_list = []

print(f"Number of prompts:{len(promptList)} Expected number of tweets:{len(promptList) * tweetNum}")

for j in range(len(promptList)):
    startTime = time.time()
    # Using TwitterSearchScraper to scrape data and append tweets to list
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(promptList[j]).get_items()):
        if i == tweetNum:
            break
        tweets_list.append([tweet.date, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])

    # Creating a dataframe from the tweets list above
    tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Text', 'Username'])

    with open('tweets.csv', 'w', encoding='utf-8-sig') as file:
        tweets_df.to_csv(file)

    endTime = time.time()
    elapsedTime = endTime - startTime
    totalTime = totalTime + elapsedTime
    totalTweets = totalTweets + tweetNum
    print(f"Pulled {tweetNum} tweets for prompt {promptList[j]} in: {elapsedTime}s")

print(f"\nPulled a total of {totalTweets} in {totalTime/60}m for an average of {totalTweets/totalTime}t/s")