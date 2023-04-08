import time

import snscrape.modules.twitter as sntwitter
from snscrape.base import ScraperException

retries = 0
tweetList = []
#scrapedTweets = sntwitter.TwitterSearchScraper('shopping since:2013-12-16 until:2015-12-16').get_items()

while True:
    keepRetrying = True

    try:
        scrapedTweets = sntwitter.TwitterSearchScraper('shopping since:2013-12-16 until:2015-12-16').get_items()
        for tweet in scrapedTweets:
            print("\rProcessed " + str(len(tweetList)) + " tweets so far", end='')
            
            if len(tweetList) >= 100:
                keepRetrying = False
                break

            tweetList.append([tweet.date, tweet.rawContent.replace('\n', ' ').replace('\r', '').strip(), tweet.user.username])

    except ScraperException as e:
        # If a ScraperException is raised, sleep for a few seconds and retry
        print("\nEncountered a ScraperException. Retrying tweet in a minute...")
        retries += 1
        time.sleep(60)

    if not keepRetrying:
        break

#print("\nTweet list 1:\n", tweetList)
print(f"\rScraped {len(tweetList)} tweets with {retries} retries")