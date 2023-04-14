from multiprocessing.connection import wait
import pandas as pd
import snscrape.modules.twitter as sntwitter
from snscrape.base import ScraperException
import datetime as dt
import contextlib
import time
import threading
import re
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
global workersWaiting
workersWaiting = 0
global setRate
setRate = 0
global scraperManagerStatus
scraperManagerStatus = "Nothing"
global rateManagerStatus
rateManagerStatus = "Nothing"
global semaphoreOffset
semaphoreOffset = 0

# A lock for manipulating the global variables
threadLock = threading.Lock()
# A lock exclusively for writing to the output file
writeLock = threading.Lock()
threadsStop = threading.Event()
waitBlock = threading.Condition()
rateSemaphore = threading.Semaphore(50)
# Two threading events to keep track of our 429 status
toManyRequests = threading.Event()
stillToManyRequests = threading.Event()


# TODO should be moved to JSON file
### CONFIGURATION VARIABLES ###


# The number of tweet we want to pull for each prompt and period
global desiredRate
desiredRate = 150
global tweetNum
tweetNum = 100
# Start date in Y/M/D format
startDate = [2013, 1, 1]
# Words or phrases to search for
searchPhrases = ['vacation', 'outing', 'travel', 'alone', 'loneliness']
#searchPhrases = ['depressed', 'stressed', 'happy', 'sad', 'joyful']
#searchPhrases = ['blissful', 'food', 'snack']


### FUNCTION CREATION ###


def workersWaitingIncrement():
    global workersWaiting

    with threadLock:
        workersWaiting += 1


def workersWaitingDecrement():
    global workersWaiting

    with threadLock:
        workersWaiting -= 1


def timeStamped(fname, fmt='%Y-%m-%d-%H-%M-%S_{fname}'):
    return dt.datetime.now().strftime(fmt).format(fname=fname)


# TODO Possibly surround the write in a try catch in case the write fails for some reason
def writeTweetListToFile(outputFilePath, tweetList):
    global totalTweetsWritten

    # Turn the tweet list into a data frame
    tweetsDF = pd.DataFrame(tweetList, columns=['time_stamp', 'tweet_id', 'content', 'author'])
    
    # Insert the data frame into out csv file
    with writeLock:
        tweetsDF.to_csv(outputFilePath, mode='a', index=False, header=False)
    
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
    with threadLock: 
        workersAlive += 1

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
        # Wait until the scraperThread manager tells the scraperThread to go again
        with waitBlock:
            waitBlock.wait()
        # Wait till it's this threads turn in the queue
        with rateSemaphore:
            workersWaitingDecrement()
            # Attempt to scrape the next tweet from Twitter and handle any exception that may occur
            try:
                # Also redirect stderr just for this call because it is so dang noisy 
                #with contextlib.redirect_stderr(None):
                    # Grab the next tweet from the iterator
                    # This can throw a ScraperException for a few reasons
                tweet = next(scrapedTweets)

                # Append the tweet we scraped to our running list of them
                tweetList.append([tweet.date, tweet.id, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])
                # Increment our tweet count so we can see if we return or not later
                localTweetsScraped += 1
                # Update the counter for how many tweets we have pulled from Twitter and stored in memory
                with threadLock:
                    totalTweetsScraped += 1

            except ScraperException as se:
                regexPattern = r"4 requests to .* failed, giving up\."
                # If a ScraperException is raised usually because of a 429 error
                if re.search(regexPattern, str(se)):
                    # If this thread was released to see if the 429 error is gone then report back if it is or not
                    if toManyRequests.is_set():
                        stillToManyRequests.set()
                    toManyRequests.set()
                    continue
                # If it was anything besides the 429 error there are other issue and we must exit
                else:
                    with threadLock:
                        workersAlive -= 1
                    with writeLock:
                        print(f"{thread.name} {str(se)}", file=sys.stderr)
                    return
            # This exception will be raised if the iterator has nothing else to give. We've exhausted related tweets for that time period
            except StopIteration as e:
                with threadLock:
                    tweetsExpected = tweetsExpected - (tweetNum - localTweetsScraped)
                writeTweetListToFile(outputFilePath, tweetList)
                with writeLock:
                    print(f"{thread.name} {prompt}\nExited early with {localTweetsScraped}/{tweetNum} tweets", file=sys.stderr)
                with threadLock:
                    workersAlive -= 1
                return
        # If we hit ten tweets then flush the list to our output file to save what we have and to avoid using to much memory
        if len(tweetList) >= 10:
            # Write the tweet list to the output file
            writeTweetListToFile(outputFilePath, tweetList)
            
            # Flush the tweets out of the tweet list
            tweetList = []

    with threadLock:
        workersAlive -= 1
    return


def semaphoreManager():
    global semaphoreOffset

    while workersAlive > 0:
        time.sleep(5)

        while threadsStop.is_set():
            time.sleep(1)

        elapsedTime = time.time() - startTime
        tweetScrappingRate = totalTweetsScraped / elapsedTime

        # Adjust the amount of semaphores based on actual scraping rate.
        if tweetScrappingRate <= (desiredRate - 1) and tweetScrappingRate > 0 and semaphoreOffset < 495:
            rateSemaphore.release()
            semaphoreOffset += 1

    return


def rateManager():
    global setRate, rateManagerStatus
    
    setRate = desiredRate

    while workersAlive > 0:
        while threadsStop.is_set():
            time.sleep(1)

        elapsedTime = time.time() - startTime
        tweetScrappingRate = totalTweetsScraped / elapsedTime

        with waitBlock:
            waitBlock.notify()

        if tweetScrappingRate <= (desiredRate - 1) and tweetScrappingRate > 0 and setRate < (desiredRate + 50):
            setRate += 1
        elif tweetScrappingRate >= (desiredRate + 1):
            setRate += 1

        time.sleep(1 / setRate)

    return


def scraperManager():
    global desiredRate, scraperManagerStatus

    toManyRequests.clear()
    stillToManyRequests.clear()

    while workersAlive > 0:
        scraperManagerStatus = "I'm Running"

        # Run this if a thread has signaled we received a 429 response
        if toManyRequests.is_set():
            # Call all worker threads to stop
            threadsStop.clear()

            # Check if there are any threads actively running
            if workersWaiting != workersAlive:
                scraperManagerStatus = "I'm waiting for threads to finish up"
                # Wait for all threads to finish running their one request and then block
                while workersWaiting != workersAlive:
                    time.sleep(1)

        # Fall into this loop until we stop receiving 429 responses
        while toManyRequests.is_set():
            # Notify a single thread to start in order to see if it still gets a 429 response
            scraperManagerStatus = "I'm Sending out one thread"
            with waitBlock:
                waitBlock.notify()
            # Wait for that thread to finish
            if workersWaiting != workersAlive:
                while workersWaiting != workersAlive:
                    time.sleep(1)

            # Check if that thread got a 429 response
            if stillToManyRequests.is_set():
                scraperManagerStatus = "I'm waiting for 5m"
                # Clear the flag and wait for five minutes, decrease the rate
                time.sleep(60 * 5)
                if desiredRate > 70:
                    desiredRate -= 5
                stillToManyRequests.clear()
            else:
                toManyRequests.clear()

        # ThreadsStop is set then that means we just got done taking care of 429 errors and we can now clear it
        if threadsStop.is_set():
            threadsStop.clear()

    scraperManagerStatus = "I exited"

    return


def displayManager():
    # Check if there are any scraperThreads left to monitor
    while workersAlive > 0:
        # Calculate our display variables
        elapsedTime = time.time() - startTime
        tweetScrappingRate = totalTweetsScraped / elapsedTime

        # Clear the screen to make room for the UI
        os.system('cls' if os.name == 'nt' else 'clear')

        # Print status updates
        if toManyRequests.is_set():
            print("Status: 429 error received, slowing down")
        elif stillToManyRequests.is_set():
            print("Status: Rate limited by Twitter, sleeping for 5m")
        else:
            print("Status: Nominal")

        print(f"Scraper manager status: {scraperManagerStatus}")

        print(f"Rate manager status: {rateManagerStatus}")

        print(f"Threads alive/Threads spawned: {workersAlive}/{workersSpawned}")

        print(f"Threads waiting/Threads alive: {workersWaiting}/{workersAlive}")

        print(f"Tweets scraped: {totalTweetsScraped}")

        print(f"Tweets written out of tweets expected: {totalTweetsWritten}/{tweetsExpected}")

        if workersAlive == 0:
            print(f"Average scraperThread completion: 100%")
        else:
            averageBlockCompletion = totalTweetsScraped/workersSpawned/tweetNum*100
            print(f"Average block completion: {averageBlockCompletion}%")

        if tweetScrappingRate == 0:
            print(f"Estimated time till next block write: Inf!")
        elif workersAlive == 0:
            print(f"Estimated time till next block write: N/A")
        else:
            print(
                f"Estimated time till done: {(tweetNum-(totalTweetsScraped/workersAlive))/tweetScrappingRate*workersAlive/60}m")
        print(f"Set rate/Desired rate: {setRate}/{desiredRate}")
        print(f"Semaphores avalible: {50 + semaphoreOffset}")
        print(f"Current/Desired scraping rate: {tweetScrappingRate}tps/{desiredRate}tps")

        time.sleep(1)

    return


### MAIN BODY ###


# Start the performance timer
startTime = time.time()

if not os.path.exists('logs'):
    os.makedirs('logs')

logFile = timeStamped('errorLog.txt')
logFilePath = os.path.join('logs', logFile)

# Open a file to use as a log
with open(logFilePath, 'w') as logFile:
    # Redirect sterr to log file
    sys.stderr = logFile

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

    # Create and start the thread that will be coordinating all of the scraper threads
    managerThread = threading.Thread(target=scraperManager, args=())
    managerThread.name = "ManagerThread"
    managerThread.start()

    rateManagerThread = threading.Thread(target=rateManager, args=())
    rateManagerThread.name = "RateManagerThread"
    rateManagerThread.start()

    # Create and start the thread that will be adjusting the amount of semaphores
    semaphoreManagerThread = threading.Thread(target=semaphoreManager, args=())
    semaphoreManagerThread.name = "SemaphoreManagerThread"
    semaphoreManagerThread.start()

    # Create and start the thread that will be running the data display
    displayThread = threading.Thread(target=displayManager)
    displayThread.name = "DisplayThread"
    displayThread.start()

    # Join all threads that were created
    for scraperThread in scraperThreads:
        scraperThread.join()

    managerThread.join()
    rateManagerThread.join()
    semaphoreManagerThread.join()
    displayThread.join()

    # Performance monitoring stuff
    endTime = time.time()
    elapsedTime = endTime - startTime

print(
    f"\nPulled a total of {totalTweetsWritten} in {elapsedTime/60}m for an average of {totalTweetsWritten/elapsedTime}t/s")
