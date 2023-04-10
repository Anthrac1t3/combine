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


global startTime
startTime = 0

global totalTweetsScraped
totalTweetsScraped = 0
# A counter for the total number of tweets we pull
global totalTweetsWritten
totalTweetsWritten = 0
# How many tweets we expect to return based on the number of prompts and the number of tweets to pull for each prompt
global tweetsExpected
tweetsExpected = 0
# How many threads(prompts) that we spawned
global threadsSpawned
threadsSpawned = 0
# How many threads have not been .join()ed at the moment
global threadsAlive
threadsAlive = 0
# How many threads are currently sleeping because they received a 429 return code
global threadsSleeping
threadsSleeping = 0
# A handy tool that will help us later ;)
threadLock = threading.Lock()
# A lock exclusively for writing to the output file
writeLock = threading.Lock()
waitLock = threading.Condition()


### CONFIGURATION VARIABLES ###


# Minutes to delay between requests after getting a 429 error
global startChance
startChance = 5
# The number of tweet we want to pull for each prompt and period
global tweetNum
tweetNum = 100
# Start date in Y/M/D format
startDate = [2020, 1, 1]
# Words or phrases to search for
searchPhrases = ["$SPY", "$VIX", "$QQQ"]
# Number of time to wait five minutes and then retry the prompt after hitting 429 error
retryLimit = 3


### FUNCTION CREATION ###


def generateDateList(startYear, startMonth, startDay):
    #startTime = time.time()
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

    # Performance monitoring stuff
    #endTime = time.time()
    #elapsedTime = endTime - startTime
    #print(f"Generated date list in: {elapsedTime}s")
    return dateList


def generatePromptList(phrases, dateList):
    #startTime = time.time()
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

    # Performance monitoring stuff
    #endTime = time.time()
    #elapsedTime = endTime - startTime
    #print(f"Generated prompt list in: {elapsedTime}s")
    return promptList


def scrapeTweets(prompt):
    global totalTweetsScraped, tweetNum, totalTweetsWritten, threadsSleeping, startChance

    # Sleep for a random number of seconds at the beginning in order to stagger the start of all the threads
    time.sleep(randint(0, 60))
    tweetCount = 0
    tweetsList = []

    while True:
        try:
            # Create a tweet generator
            scrapedTweets = sntwitter.TwitterSearchScraper(prompt).get_items()

            # Iterate through that generator until we reach the number of tweets we need
            for tweet in scrapedTweets:
                #Wait until the thread manager tells the thread to go
                with waitLock:
                    waitLock.wait()
                    
                #Append the tweet we scraped to our running list of them
                tweetsList.append([tweet.date, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])
                
                #Increment our tweet count so we can see if we return or not later
                tweetCount += 1

                #Adjust the slowdown factor if the request succeeded
                with threadLock:
                    totalTweetsScraped += 1
                    if startChance > 1:
                        startChance -= 1

                    # If we hit ten tweets then flush the list to our output file
                    if len(tweetsList) >= 10:
                        # Turn the tweet list into a data frame
                        tweets_df = pd.DataFrame(tweetsList, columns=['Datetime', 'Text', 'Username'])

                        # Insert the data frame into out csv file
                        tweets_df.to_csv('tweets.csv', mode='a', index=False, header=False)

                        # Record the number of tweets we grabbed
                        totalTweetsWritten += len(tweetsList)
                        
                        # Flush the tweets out of the tweet list
                        tweetsList = []

                        # Check if we hae collected the specified number of tweets from this prompt and if so then return
                        if tweetCount >= tweetNum:
                            return

        # If a ScraperException is raised.
        #TODO Make it notify the manager thread that a 429 was recived
        except ScraperException as e:
            pass

def scraperManager():
    while threading.active_count() > 2:
        pass

    return

#TODO Have there be no calculations in the thread lock make it so all we do is copy the current values and release
def displayManager():
    global startTime, tweetNum, totalTweetsScraped, totalTweetsWritten, threadsAlive, threadsSpawned

    # Make copies of all the read-only, static values
    with threadLock:
        startTimecp = startTime
        tweetNumcp = tweetNum
        threadsSpawnedcp = threadsSpawned
        tweetsExpected = tweetNumcp * threadsSpawnedcp

    # Check if there are any threads left to monitor
    while threading.active_count() > 1:
        # Make copies of all the read-only, updateable variables
        with threadLock:
            totalTweetsScrapedcp = totalTweetsScraped
            totalTweetsWrittencp = totalTweetsScraped
        
        # Calculate our display variables
        elapsedTime = time.time()-startTimecp


        with threadLock:
            # Clear the screen to make room for the UI
            os.system('cls' if os.name == 'nt' else 'clear')

            #Print status updates
            print(f"Alive Threads: {threadsAlive}/{threadsSpawned}")
            
            print(f"Tweets scraped: {totalTweetsScraped}")
            
            print(f"Tweets written out of tweets expected: {totalTweetsWritten}/{tweetsExpected}")
            
            if threadsAlive == 0:
                print(f"Average thread completion: 100%")
            else:
                averageBlockCompletion = totalTweetsScraped/threadsAlive/tweetNum*100
                print(f"Average block completion: {averageBlockCompletion}%")
            
            
            tweetScrappingRate = totalTweetsScraped/elapsedTime
            
            if tweetScrappingRate == 0:
                print(f"Estimated time till next block write: Inf!")
            elif threadsAlive == 0:
                print(f"Estimated time till next block write: N/A")
            else:
                print(f"Estimated time till done: {(tweetNum-(totalTweetsScraped/threadsAlive))/tweetScrappingRate*threadsAlive/60}m")
            
            print(f"Current tweet scraping rate: {tweetScrappingRate}t/s")

        time.sleep(1)

    return


### MAIN BODY ###


#Redirect stderr to null you can uncomment this if you want but it's a headache to look at
nullTerm = open(os.devnull, 'w')
sys.stderr = nullTerm

dateList = generateDateList(startDate[0], startDate[1], startDate[2])
promptList = generatePromptList(searchPhrases, dateList)

#print(f"Number of prompts:{len(promptList)} Expected number of tweets:{len(promptList) * tweetNum}")

tweetsExpected = len(promptList) * tweetNum
threadsSpawned = len(promptList)

# Creating and starting threads for each prompt
startTime = time.time()
#startTimegl = startTime

# Create a handy list for all the other threads
threads = []
# Now spawn a thread for every prompt in the prompt list and start it
for prompt in promptList:
    with threadLock:
        threadsAlive += 1
    thread = threading.Thread(target=scrapeTweets, args=(prompt,))
    threads.append(thread)
    thread.start()

# Create and start the thread tha will be running the UI
uiThread = threading.Thread(target=displayManager)
uiThread.start()

# Waiting for all threads to finish before continuing
for thread in threads:
    thread.join()
    with threadLock:
        threadsAlive -= 1

# Wait for the UI thread to close up gracefully
uiThread.join()

# Performance monitoring stuff
endTime = time.time()
elapsedTime = endTime - startTime

print(f"\nPulled a total of {totalTweetsWritten} in {elapsedTime/60}m for an average of {totalTweetsWritten/elapsedTime}t/s")
