# combine
A multithreading wrapper and general abstracter for snscrape

Only TwitterScraperThreads works at the moment

run with "py .\TwitterScraperThreads.py" on Windows or "python3 TwitterScraperThreads.py" on BASH

Configurable variables for TwitterScraperThreads.py include:
desiredRate, which is the number of tweets per second you would like to pull (in practice Twitter limits you to about 100tps)

tweetNum, which is the number of tweets you want to extract for each prompt

startDate, which is the earliest tweets you want

searchPhrases, which are the keywords or phrases you are searching for