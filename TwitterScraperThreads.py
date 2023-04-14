import contextlib
import csv
import datetime as dt
import json
import os
#import re
import sys
import threading
import time
from random import *

#import pandas as pd
import snscrape.modules.twitter as sntwitter
from snscrape.base import ScraperException


### GLOBAL DATA STORES ###


# The time that we started pulling tweets. Used in performance and rate calculation
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
# How many workers are idle and waiting to scrape a tweet
global workersWaiting
workersWaiting = 0
# The actual rate that the rate manager is letting threads go at
global setRate
setRate = 0

global scraperManagerStatus
scraperManagerStatus = "Nothing"

# Boolean to keep track of if we need to stop running any threads
global threadsStop
threadsStop = True

# Two booleans to keep track of our 429 status
global toManyRequests
toManyRequests = True
global stillToManyRequests
stillToManyRequests = False

# A lock for manipulating the global variables
threadLock = threading.Lock()
flagLock = threading.Lock()
# A lock exclusively for writing to the output file
writeLock = threading.Lock()
# A block that threads can wait on until the rate manager tells them to go
waitBlock = threading.Condition()
# A semaphore to help control bursts of threads
scraperSemaphore = threading.BoundedSemaphore(100)


### CONFIGURATION VARIABLES ###


# The number of tweets per second we want to pull
global desiredRate
desiredRate = 0
# The number of tweet we want to pull for each prompt and period
global tweetNum
tweetNum = 0
# Start date in Y/M/D format
global startDate
startDate = []
# Words or phrases to search for
global searchPhrases
searchPhrases = []


### FUNCTION CREATION ###


def workersAliveIncrement():
    global workersAlive

    with threadLock:
        workersAlive += 1


def workersAliveDecrement():
    global workersAlive

    with threadLock:
        workersAlive -= 1


def workersWaitingIncrement():
    global workersWaiting

    with threadLock:
        workersWaiting += 1


def workersWaitingDecrement():
    global workersWaiting

    with threadLock:
        workersWaiting -= 1


def setToManyRequests(value):
    global toManyRequests
    
    with flagLock:
        toManyRequests = value


def setStillToManyRequests(value):
    global stillToManyRequests
    
    with flagLock:
        stillToManyRequests = value


def loadSettings(fileName):
    global desiredRate, tweetNum, startDate, searchPhrases
    
    #Open the json file, load it's contents into a JSON object
    with open(fileName, 'r') as file:
        settings = json.load(file)

    desiredRate = settings['desiredRate']
    tweetNum = settings['tweetNum']
    startDate = settings['startDate']
    searchPhrases = settings['searchPhrases']


def timeStamped(fname, fmt='%Y-%m-%d-%H-%M-%S_{fname}'):
    return dt.datetime.now().strftime(fmt).format(fname=fname)


# TODO Possibly surround the write in a try catch in case the write fails for some reason
def writeTweetListToFile(outputFilePath, tweetList):
    global totalTweetsWritten

    with writeLock:
        with open(outputFilePath, 'a', encoding='UTF8', newline='') as file:
            writer = csv.writer(file)

            for tweet in tweetList:
                writer.writerow(tweet)

    # Record the number of tweets we grabbed
    with threadLock:
        totalTweetsWritten += len(tweetList)
    
    # Return the number of tweets written
    return len(tweetList)


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
    global workersAlive, workersWaiting, totalTweetsScraped, tweetsExpected

    # Let the manager know you're alive
    workersAliveIncrement()

    # Set this so that the thread can reference itself
    thread = threading.current_thread()

    # Counter to keep track of the tweets that this single thread was able to get
    localTweetsScraped = 0
    # A blank list to store tweets that have been scraped but not written
    tweetList = []

    # Creating the path of the output file so that tweets of the same search phrase are grouped into one .csv file
    phrase = prompt.split('since')[0].strip()
    outputFile = str(phrase) + ".csv"
    outputFilePath = os.path.join('results', outputFile)

    # Create a tweet iterator
    scrapedTweets = sntwitter.TwitterSearchScraper(prompt).get_items()

    # Run until we have collected the required amount of tweets
    while localTweetsScraped <= tweetNum:
        workersWaitingIncrement()
        # See if there is a connection available to take
        #scraperSemaphore.acquire(blocking=True)
        # Wait until the scraperThread manager tells the scraperThread to go
        with waitBlock:
            waitBlock.wait()
        workersWaitingDecrement()

        # Attempt to scrape the next tweet from Twitter and handle any exception that may occur
        try:
            # Also redirect stderr just for this call because it is so dang noisy 
            with contextlib.redirect_stderr(None):
                # Grab the next tweet from the iterator
                # This can throw a ScraperException for a few reasons
                tweet = next(scrapedTweets)

            setToManyRequests(False)

            #Release the connection back to the pool
            #scraperSemaphore.release()

            # Append the tweet we scraped to our running list of them
            tweetList.append([tweet.date, tweet.id, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])
            # Increment our tweet count so we can see if we return or not later
            localTweetsScraped += 1
            # Update the counter for how many tweets we have pulled from Twitter and stored in memory
            with threadLock:
                totalTweetsScraped += 1

        except ScraperException as se:
            #scraperSemaphore.release()
            if toManyRequests:
                setStillToManyRequests(True)
            else:
                setToManyRequests(True)
            continue
        except StopIteration as si:
            #scraperSemaphore.release()
            if toManyRequests:
                setStillToManyRequests(True)
            else:
                setToManyRequests(True)
            continue
        # Any other exception gets caught here
        except BaseException as e:
            workersAliveDecrement()
            with writeLock:
                print(f"{thread.name} {str(e)}", file=sys.stderr)
            return
        # If we hit ten tweets then flush the list to our output file to save what we have and to avoid using to much memory
        if len(tweetList) >= 10:
            # Write the tweet list to the output file
            writeTweetListToFile(outputFilePath, tweetList)
            
            # Flush the tweets out of the tweet list
            tweetList = []

    workersAliveDecrement()
    return


def rateAdjuster():
    global setRate

    setRate = 5

    while workersAlive > 0:
        while threadsStop:
            time.sleep(1)

        elapsedTime = time.time() - startTime
        tweetScrappingRate = totalTweetsScraped / elapsedTime

        if tweetScrappingRate <= (desiredRate - 1) and tweetScrappingRate > 0 and setRate < (desiredRate + 25):
            setRate += 1
        elif tweetScrappingRate >= (desiredRate + 1) and setRate > 1:
            setRate -= 1

    time.sleep(5)

    return


def scraperAdmitter():
    while workersAlive > 0:
        
        while threadsStop:
            time.sleep(1)

        with waitBlock:
            #waitBlock.notify(max(round(workersAlive / 10), 1))
            waitBlock.notify(5)

        if setRate == 0:
            time.sleep(1)
        else:
            time.sleep(1 / setRate)

    return


def scraperManager():
    global desiredRate, scraperManagerStatus, threadsStop

    while workersAlive > 0:
        scraperManagerStatus = "I'm Running"

        # Run this if a thread has signaled we received a 429 response
        if toManyRequests:
            # Call all worker threads to stop
            threadsStop = True

            # Check if there are any threads actively running
            if workersWaiting != workersAlive:
                scraperManagerStatus = "I'm waiting for threads to finish up"
                # Wait for all threads to finish running their one request and then block
                while workersWaiting != workersAlive:
                    time.sleep(1)

        # Fall into this loop until we stop receiving 429 responses
        while toManyRequests:
            # Notify a single thread to start in order to see if it still gets a 429 response
            scraperManagerStatus = "I'm Sending out one thread"
            with waitBlock:
                waitBlock.notify(1)
            time.sleep(5)
            # Wait for that thread to finish
            if workersWaiting != workersAlive:
                scraperManagerStatus = "I'm waiting for threads to finish up"
                while workersWaiting != workersAlive:
                    time.sleep(1)

            # Check if that thread got a 429 response
            if stillToManyRequests:
                setStillToManyRequests(False)
                scraperManagerStatus = "I'm waiting for 5m"
                # Clear the flag and wait for five minutes, decrease the rate
                time.sleep(60 * 5)
                if desiredRate > 75:
                    desiredRate -= 1
            else:
                setToManyRequests(False)

        # ThreadsStop is set then that means we just got done taking care of 429 errors and we can now clear it
        if threadsStop:
            threadsStop = False

    scraperManagerStatus = "I exited"

    return


def displayManager():

    status = "Nominal"
    averageBlockCompletion = 0
    timeTillDone = "Inf!"

    # Check if there are any scraperThreads left to monitor
    while workersAlive > 0:
        # Calculate our display variables
        elapsedTime = time.time() - startTime
        tweetScrappingRate = totalTweetsScraped / elapsedTime

        # Determine status of our two booleans
        if toManyRequests:
            toManyRequestsStatus = "Set"
        else:
            toManyRequestsStatus = "Cleared"
        if stillToManyRequests:
            stillToManyRequestsStatus = "Set"
        else:
            stillToManyRequestsStatus = "Cleared"

        # Calculate block completion
        if workersAlive <= 0:
            averageBlockCompletion = 100
        else:
            averageBlockCompletion = totalTweetsScraped/workersSpawned/tweetNum*100

        # Calculate the estimated time till done
        if tweetScrappingRate <= 0:
            timeTillDone = "Inf!"
        elif workersAlive <= 0:
            timeTillDone = "N/A"
        else:
            timeTillDone = str((tweetNum-(totalTweetsScraped/workersAlive))/tweetScrappingRate*workersAlive/60) + 'm'

        # Clear the screen to make room for the UI
        os.system('cls' if os.name == 'nt' else 'clear')

        # Print status updates
        print(f"toManyRequests status: {toManyRequestsStatus}")

        print(f"stillToManyRequests status: {stillToManyRequestsStatus}")

        print(f"Scraper manager status: {scraperManagerStatus}")

        print(f"Threads alive/Threads spawned: {workersAlive}/{workersSpawned}")

        print(f"Threads waiting/Threads alive: {workersWaiting}/{workersAlive}")

        print(f"Tweets scraped: {totalTweetsScraped}")

        print(f"Tweets written out of tweets expected: {totalTweetsWritten}/{tweetsExpected}")

        print(f"Average block completion: {averageBlockCompletion}%")

        print(f"Estimated time till done: {timeTillDone}")
        
        print(f"Set rate/Desired rate: {setRate}/{desiredRate}")

        print(f"Current/Desired scraping rate: {tweetScrappingRate}tps/{desiredRate}tps")

        time.sleep(1)

    return


### MAIN BODY ###


if not os.path.exists('logs'):
    os.makedirs('logs')

logFile = timeStamped('errorLog.txt')
logFilePath = os.path.join('logs', logFile)

# Open a file to use as a log
with open(logFilePath, 'w') as logFile:
    # Redirect sterr to log file
    sys.stderr = logFile

    # Load the settings
    try:
        loadSettings('config.json')
    except BaseException as e:
        print("ERROR: Could not load settings", file=sys.stderr)

    # Create the results folder if it's not already there
    if not os.path.exists('results'):
        os.makedirs('results')

    dateList = generateDateList(startDate[0], startDate[1], startDate[2])
    promptList = generatePromptList(searchPhrases, dateList)

    tweetsExpected = len(promptList) * tweetNum

    # Creating and starting scraperThreads for each prompt
    # Create a handy list for all the other scraperThreads
    scraperThreads = []
    # Now spawn a scraperThread for every prompt in the prompt list and start it
    # TODO this might need to be in a try catch block to catch some kind of malformed query or something
    for i, prompt in enumerate(promptList):
        scraperThread = threading.Thread(target=scrapeTweets, args=(prompt,))
        scraperThreads.append(scraperThread)
        # Name the thread for debugging purposes
        scraperThread.name = "WorkerThread:" + str(i)
        scraperThread.start()
        workersSpawned += 1

    # Start the performance timer
    startTime = time.time()

    # Create and start the thread that will be coordinating all of the scraper threads
    managerThread = threading.Thread(target=scraperManager, args=())
    managerThread.name = "ManagerThread"
    managerThread.start()

    # Create and start the rate adjuster thread
    rateAdjusterThread = threading.Thread(target=rateAdjuster, args=())
    rateAdjusterThread.name = "RateAdjusterThread"
    rateAdjusterThread.start()

    # Start the scraper admitter thread
    scraperAdmitterThread = threading.Thread(target=scraperAdmitter, args=())
    scraperAdmitterThread.name = "RateManagerThread"
    scraperAdmitterThread.start()

    # Create and start the thread that will be running the data display
    displayThread = threading.Thread(target=displayManager)
    displayThread.name = "DisplayThread"
    displayThread.start()

    # Join all threads that were created
    for scraperThread in scraperThreads:
        scraperThread.join()

    managerThread.join()
    rateAdjusterThread.join()
    scraperAdmitterThread.join()
    displayThread.join()

    # Performance monitoring stuff
    endTime = time.time()
    elapsedTime = endTime - startTime

print(f"\nPulled a total of {totalTweetsWritten} in {elapsedTime/60}m for an average of {totalTweetsWritten/elapsedTime}t/s")
