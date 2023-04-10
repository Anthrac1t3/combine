from multiprocessing.connection import wait
import pandas as pd
import snscrape.modules.twitter as sntwitter
from snscrape.base import ScraperException
import datetime as dt
import time
import threading
import os
import sys
from random import *


### GLOBAL DATA STORES ###


# The time that we started pulling tweets. Used in oerformanc and rate calculation
global startTime
startTime = 0
# Counter for the number of tweets we have scraped from Twitter so far
global totalTweetsScraped
totalTweetsScraped = 0
# A counter for the total number of tweets we have written to the output csv/s
global totalTweetsWritten
totalTweetsWritten = 0
# How many tweets we expect to return based on the number of prompts and the number of tweets to pull for each prompt
global tweetsExpected
tweetsExpected = 0
# How many scraperThreads(prompts) that we spawned
global workersSpawned
workersSpawned = 0
# How many scraperThreads are alive at any given time
global workersAlive
workersAlive = 0
# A handy tool that will help us later ;)
threadLock = threading.Lock()
# A lock exclusively for writing to the output file
writeLock = threading.Lock()
waitLock = threading.Condition()
# Two threading events to keep track of our 429 status
toManyRequests = threading.Event()
stillToManyRequests = threading.Event()


#TODO should be moved to JSON file
### CONFIGURATION VARIABLES ###


# The number of tweet we want to pull for each prompt and period
global tweetNum
tweetNum = 100
# Desired scraping rate in tweets per second
initialRate = 100
# Start date in Y/M/D format
startDate = [2022, 1, 1]
# Words or phrases to search for
searchPhrases = ["$SPY", "$VIX", "$QQQ"]


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


def scrapeTweets(prompt):
    global toManyRequests, stillToManyRequests, totalTweetsScraped, totalTweetsWritten
    
    localTweetsScraped = 0
    scrapedTweetsList = []

    # Create a tweet iterator
    scrapedTweets = sntwitter.TwitterSearchScraper(prompt).get_items()

    while localTweetsScraped <= tweetNum:
        #Wait until the scraperThread manager tells the scraperThread to go
        with waitLock:
            waitLock.wait()
        
        # Attempt to scrape the next tweet from Twitter and handle any exception that may occur
        try:
            # Grab the next tweet from the iterator
            # This can throw a ScraperException for a few reasons
            tweet = next(scrapedTweets)

            #Append the tweet we scraped to our running list of them
            scrapedTweetsList.append([tweet.date, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])
            # Clear the 429 flag since we we're able to get a request through
            toManyRequests.clear()
            #Increment our tweet count so we can see if we return or not later
            localTweetsScraped += 1
            # Update the counter for how many tweets we have pulled from Twitter and stored in memory
            with threadLock:
                totalTweetsScraped += 1
        
        # If a ScraperException is raised.
        #TODO Make it notify the manager scraperThread that a 429 was received and exits for any other scraper exception
        except ScraperException as e:
            # If this thread was released to see if the 429 error is gone then report back if it is or not
            if toManyRequests.is_set():
                stillToManyRequests.set()
            print(e)
            return
            #continue

        # If we hit ten tweets then flush the list to our output file
        if len(scrapedTweetsList) >= 10:
            # Turn the tweet list into a data frame
            tweets_df = pd.DataFrame(scrapedTweetsList, columns=['Datetime', 'Text', 'Username'])

            #TODO Possibly surround this in a try catch in case the write fails for some reason
            # Insert the data frame into out csv file
            with writeLock:
                tweets_df.to_csv('tweets.csv', mode='a', index=False, header=False)

            # Record the number of tweets we grabbed
            with threadLock:
                totalTweetsWritten += len(scrapedTweetsList)
            
            # Flush the tweets out of the tweet list
            scrapedTweetsList = []

    return


def scraperManager(initialRate):
    rate = initialRate

    while workersAlive > 0:
        # Fall into this loop if we recive a 429 error
        while toManyRequests.is_set():
            with waitLock:
                waitLock.notify()
            if stillToManyRequests.is_set():
                # Clear the flag and wait for five minutes, decrease the rate
                stillToManyRequests.clear()
                time.sleep(60 * 5)
                rate -= 10
        
        # Let the threads go one by one at a controlled rate
        interval = 1 / rate
        time.sleep(interval)
        with waitLock:
            waitLock.notify()

    return


#TODO Have there be no calculations in the scraperThread lock make it so all we do is copy the current values and release
def displayManager():
    tweetsExpected = tweetNum * workersSpawned

    # Check if there are any scraperThreads left to monitor
    while workersAlive > 0:
        # Calculate our display variables
        elapsedTime = time.time() - startTime
        tweetScrappingRate = totalTweetsScraped / elapsedTime

        with threadLock:
            # Clear the screen to make room for the UI
            os.system('cls' if os.name == 'nt' else 'clear')

            #Print status updates
            print(f"Alive Threads: {workersAlive}/{workersSpawned}")
            
            print(f"Tweets scraped: {totalTweetsScraped}")
            
            print(f"Tweets written out of tweets expected: {totalTweetsWritten}/{tweetsExpected}")
            
            if workersAlive == 0:
                print(f"Average scraperThread completion: 100%")
            else:
                averageBlockCompletion = totalTweetsScraped/workersAlive/tweetNum*100
                print(f"Average block completion: {averageBlockCompletion}%")
            
            if tweetScrappingRate == 0:
                print(f"Estimated time till next block write: Inf!")
            elif workersAlive == 0:
                print(f"Estimated time till next block write: N/A")
            else:
                print(f"Estimated time till done: {(tweetNum-(totalTweetsScraped/workersAlive))/tweetScrappingRate*workersAlive/60}m")
            
            print(f"Current tweet scraping rate: {tweetScrappingRate}t/s")

        time.sleep(1)

    return


### MAIN BODY ###


# Start the performance timer
startTime = time.time()

#Redirect stderr to null you can uncomment this if you want but it's a headache to look at
#TODO reenable these lines to redirect stderror and then hopefully to a log file insted of null term
#nullTerm = open(os.devnull, 'w')
#sys.stderr = nullTerm

dateList = generateDateList(startDate[0], startDate[1], startDate[2])
promptList = generatePromptList(searchPhrases, dateList)

tweetsExpected = len(promptList) * tweetNum

# Creating and starting scraperThreads for each prompt
# Create a handy list for all the other scraperThreads
scraperThreads = []
# Now spawn a scraperThread for every prompt in the prompt list and start it
#TODO this might need to be in a try catch block to catch some kind of malformed query or something
for prompt in promptList:
    scraperThread = threading.Thread(target=scrapeTweets, args=(prompt,))
    scraperThreads.append(scraperThread)
    scraperThread.start()
    # Increment our worker number trackers
    workersAlive += 1
    workersSpawned += 1

# Create and start the thread that will be cordinationg and directing all of the scraper threads
managerThread = threading.Thread(target=scraperManager, args=(initialRate,))
managerThread.start()
# Create and start the thread that will be running the data display
#TODO reenable these to turn the data display back on
#displayThread = threading.Thread(target=displayManager)
#displayThread.start()

# Join all threads that were created
for scraperThread in scraperThreads:
    scraperThread.join()
    workersAlive -= 1

managerThread.join()
#TODO Renable
#displayThread.join()

# Performance monitoring stuff
endTime = time.time()
elapsedTime = endTime - startTime

print(f"\nPulled a total of {totalTweetsWritten} in {elapsedTime/60}m for an average of {totalTweetsWritten/elapsedTime}t/s")
