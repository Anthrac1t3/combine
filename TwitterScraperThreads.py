import snscrape.modules.twitter as sntwitter
from snscrape.base import ScraperException
import datetime as dt
import time
import csv
import os
import threading
import sys
import contextlib
import argparse


### GLOBAL ITEMS AND INSTANCES ###


# A lock for manipulating the global variables
threadLock = threading.Lock()
# A lock reserved for writing to files
writeLock = threading.Lock()


### GLOBAL DATA STORES ###


global startTime
startTime = time.time()

def elapsedTime():
    return time.time() - startTime

global promptCount
promptCount = 0

global expectedTweets
expectedTweets = 0

global totalTweetsWritten
totalTweetsWritten = 0

global workerTable
workerTable = {}


### CONFIGURATION VARIABLES ###


# Set the number of tweets we want to scrape for each period of time
global tweetNum
tweetNum = 1000

workerNum = 3

# The date to start scraping from in year, month, day format
startDate = [2013, 1, 1]

# Set the keywords we are going to be using
keyWords = ["depressed", "loneliness", "vacation", "blissful", "outing", "travel", "snack", "happy", "sad", "joyful", "food"]


### CLASS DEFINITIONS ###


class TweetList:
    def __init__(self, prompt, capacity):
        # The search prompt
        self.prompt = prompt

        # The search phrase all the tweets were found by
        self.phrase = prompt.split('since')[0].strip()

        # Set the maximum number of tweets we want to get for this list
        self.capacity = capacity

        # A list of tweets
        self.list = []

    def append(self, i):
        self.list.append(i)

    def length(self):
        return len(self.list)


### FUNCTION DEFINITIONS ###


def timeStamped(fname, fmt='%Y-%m-%d-%H-%M-%S_{fname}'):
    return dt.datetime.now().strftime(fmt).format(fname=fname)


#TODO handle threadID and status being null
def setThreadStatus(threadID, status):
    global workerTable
    
    with threadLock:
        workerTable.update({threadID:status})



#TODO handle start date being null
def generateDateList(startDate, endDate=dt.datetime.now(), delta = dt.timedelta(days=1)):
    # Define the starting date
    startDate = dt.datetime(startDate[0], startDate[1], startDate[2])

    # Generate a list of dates one week apart
    dateList = []
    while startDate <= endDate:
        dateList.append(startDate)
        startDate += delta
    return dateList


#TODO handle wordList being null or one single day
def generatePromptFile(phraseList, dateList):
    global promptCount
    
    # Creating a prompt list to store all our twitter search terms
    promptList = []

    # Generate the prompt list from the phraseList and dateList
    for phrase in phraseList:
        for j, date in enumerate(dateList):
            # Check if we are at the end of the list or not so that we don't get an index out of bounds error
            if j == len(dateList)-1:
                break
            # Construct the prompt in our desired format
            prompt = phrase + " since:" + date.strftime("%Y-%m-%d") + " until:" + dateList[j+1].strftime("%Y-%m-%d")
            # Append it to the list
            promptList.append(prompt)

    # Write the promptList to a file to have a place to pick up from if the program crashes
    with writeLock:
        with open("prompts.txt", "w") as file:
            for prompt in promptList:
                    if prompt != promptList[-1]:
                        file.write(prompt + "\n")
                    else:
                        file.write(prompt)

    # Set the number of prompts we have to process
    with threadLock:
        promptCount = len(promptList)

    # Return the number of prompts we have
    return len(promptList)


def loadPromptFile(filePath):
    with writeLock:
        with open(filePath, "r") as file:
            for count, line in enumerate(file):
                pass

    return count


def popPrompt():
    global promptCount

    with writeLock:
        with open("prompts.txt", "r") as file:
            promptList = file.readlines()

        try:
            prompt = promptList.pop()
        except IndexError as ie:
            return None

        with open("prompts.txt", "w") as file:
            for prompt in promptList:
                if prompt != promptList[-1]:
                    file.write(prompt)
                else:
                    file.write(prompt.strip('\n'))

    # Decrement the number of prompts left to process
    with threadLock:
        promptCount -= 1

    return prompt


def writeTweetList(tweetList:TweetList, directory = "results"):
    global totalTweetsWritten

    # Create the results folder if it's not already there
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Creating the path of the output file so that tweets of the same search phrase are grouped into one .csv file
    outputFile = tweetList.phrase + ".csv"
    outputFilePath = os.path.join(directory, outputFile)

    # Open the file for writing
    with open(outputFilePath, 'a', encoding='UTF8', newline='') as file:
        # Create the csv writer
        writer = csv.writer(file)
        # Acquire writelock and write each tweet in the list as it's own row
        with writeLock:
            for tweet in tweetList.list:
                writer.writerow(tweet)

    # Update the number of tweets written
    with threadLock:
        totalTweetsWritten += tweetList.length()

    return tweetList.length()


def runDisplay():
    # Check if there are any scraperThreads left to monitor
    while True:
        # Calculate our display variables
        tweetWritingRate = totalTweetsWritten / elapsedTime()

        # Calculate completion
        completion = totalTweetsWritten/expectedTweets*100

        # Calculate the estimated time till done
        if tweetWritingRate <= 0:
            timeTillDone = "Inf!"
        else:
            timeTillDone = str((expectedTweets-totalTweetsWritten)/tweetWritingRate/60) + 'm'
        
        # Clear the screen to make room for the UI
        os.system('cls' if os.name == 'nt' else 'clear')

        # Print status updates
        for worker, status in workerTable.items():
            print(f"{worker} status: {status}")

        print(f"Tweets written out of tweets expected: {totalTweetsWritten}/{expectedTweets}")

        print(f"Prompts left: {promptCount}")

        print(f"Current rate: {tweetWritingRate}tps")

        print(f"{completion}% complete")

        print(f"Estimated time till done: {timeTillDone}")

        print(workerTable)

        if promptCount <= 0:
            return
        

        time.sleep(1)


def fillTweetList(tweetList:TweetList):
    global expectedTweets

    retries = 0

    tweetScraper = sntwitter.TwitterSearchScraper(tweetList.prompt).get_items()

    # Using TwitterSearchScraper to scrape data and append tweets to list
    while tweetList.length() < tweetList.capacity:
        try:
            # Redirect stderr just for this call because it is so dang noisy 
            with contextlib.redirect_stderr(None):
                tweet = next(tweetScraper)

            # Clean new tweet data up and append it to the list
            tweetList.append([tweet.date, tweet.id, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])
        
        # Catch any scraper exceptions
        except ScraperException as se:
            if retries < 3:
                setThreadStatus(threading.Thread.name, "Waiting")
                print(f"Thread: {threading.Thread.name} is waiting", file=sys.stderr)
                time.sleep(60 * 5)
                retries += 1
                setThreadStatus(threading.Thread.name, "Running")
            else:
                with writeLock:
                    print(f"Prompt {tweetList.prompt} had to many retries exited early with {tweetList.length()}/{tweetList.capacity} tweets\n{str(se)}", file=sys.stderr)
                # Reduce the number of expected tweets since we exited early
                with threadLock:
                    expectedTweets -= (tweetList.capacity - tweetList.length())
                return tweetList
        # Catch any stop iteration exceptions
        except StopIteration as si:
            with writeLock:
                print(f"Prompt {tweetList.prompt} exited early with {tweetList.length()}/{tweetList.capacity} tweets\n{str(si)}", file=sys.stderr)
            # Reduce the number of expected tweets since we exited early
            with threadLock:
                expectedTweets -= (tweetList.capacity - tweetList.length())
            return tweetList

    return tweetList


def runWorker():
    while (prompt := popPrompt()) is not None:
        # Create our TweetList instance
        tweetList = TweetList(prompt, tweetNum)

        # Now fill it
        fillTweetList(tweetList)

        # Now write the tweet list to the proper file
        writeTweetList(tweetList)

    setThreadStatus(threading.Thread.name, "Done")


### BEGIN MAIN BODY ###


# Create the log directory if it does not exist
if not os.path.exists('logs'):
    os.makedirs('logs')

logFile = timeStamped('errorLog.txt')
logFilePath = os.path.join('logs', logFile)

# Open a file to use as a log
with open(logFilePath, 'w') as logFile:
    # Redirect sterr to log file
    sys.stderr = logFile

    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--resume', help='Resume file')
    args = parser.parse_args()

    # Process command line arguments
    if args.resume is not None:
        try:
            promptCount = loadPromptFile(args.resume)
        except FileNotFoundError as fnfe:
            print(f"ERROR: Save file not found")
    else:
        #generateDateList([Starting year, Starting month, Starting day])
        dateList = generateDateList(startDate)
        # Generate our prompt list from the date list
        generatePromptFile(keyWords, dateList)

    expectedTweets = promptCount * tweetNum

    workerList = []

    while len(workerList) < workerNum:
        workerThread = threading.Thread(target=runWorker)
        workerThread.name = "Worker " + str(len(workerList) + 1)
        workerList.append(workerThread)
        workerThread.start()
        workerTable[workerThread.name] = "Running"

    # Create and start the thread that will be running the data display
    displayThread = threading.Thread(target=runDisplay)
    displayThread.name = "DisplayThread"
    displayThread.start()

    # Collect all the threads we spun off before closing up
    for workerThread in workerList:
        workerThread.join()
        workerTable[workerThread.name] = "Finished"

    displayThread.join()

    # Since we successfully processed all tweets then there is no need for the save file
    os.remove("prompts.txt")