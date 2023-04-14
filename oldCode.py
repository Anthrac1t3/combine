        '''
        except ScraperException as se:
            regexPattern = r"4 requests to .* failed, giving up\."
            # If a ScraperException is raised usually because of a 429 error
            if re.search(regexPattern, str(se)):
                # If this thread was released to see if the 429 error is gone then report back if it is or not
                if toManyRequests:
                    stillToManyRequests = True
                toManyRequests = True
                continue
            # If it was anything besides the 429 error there are other issue and we must exit
            else:
                workersAliveDecrement()
                with writeLock:
                    print(f"{thread.name} {str(se)}", file=sys.stderr)
            return
        ''' 
        
        '''
        # A scrape will just randomly throw this error for no reason and I can't pin down why
        except StopIteration as e:
            with threadLock:
                tweetsExpected = tweetsExpected - (tweetNum - localTweetsScraped)
            writeTweetListToFile(outputFilePath, tweetList)
            with writeLock:
                print(f"{thread.name} {prompt}\nExited early with {localTweetsScraped}/{tweetNum} tweets", file=sys.stderr)
            workersAliveDecrement()
            return
        '''