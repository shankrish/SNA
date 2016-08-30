import tweepy
import twitterkey
import time


def handle_followers_id_exception(cursor):
    """ Method to retrieve the twitter ids and catch the cursor exceptions"""
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)    #Hit the ratelimit. Sleep for 15 minutes...
        except tweepy.TweepError, error:
            print "Skipping....." , type(error), str(error)   # Not authorized-return...
            return 

# Get the auth/consumer keys from the dictionary and do web authorize...
keys = twitterkey.secretKey()
auth = tweepy.OAuthHandler(keys['consumerKey'],keys['consumerSecret'])
auth.set_access_token(keys['accessKey'],keys['accessSecret'])

api = tweepy.API(auth)   # Set the auth handler and get the tweepy request handler

try:
    
    first_id = raw_input("Enter the Twitter Id: ")
    curr_id_list = [int(first_id)]   # Initialize the list with the first Id
    print curr_id_list

    fileDescriptor = open('output2.txt','a')

    # idFollowersList holds the complete list received from followers/id api call.
    # lookupFinalList holds the list of ids with follower count < 400
    idFollowersList = []
    lookupFinalList = []


    while(curr_id_list!=None):
        # Pop the first element from the curr_id_list to make the followers/id call.
        curr_id = curr_id_list[0]
        del curr_id_list[0]
        del idFollowersList[:]  #Clear the idFollowersList

        
        # followers/id api call
        for items in handle_followers_id_exception(tweepy.Cursor(api.followers_ids, id=curr_id).pages()):
            if items == None:
                continue
            idFollowersList = idFollowersList + items
    

        # Lookup users only support a list of len 100. Split the idFollowersList
        # into list of 100s.
        indxLists = [(x,x+100) for x in range(len(idFollowersList)) if x%100==0]


        # Loop the indxLists and call the lookup user api to retrieve the user objects
        # for entries in the idFollowersList. Populate the lookupFinalList with
        # user ids whose followers count < 400
        for indxCtr in indxLists:
            try:
                lookupList = api.lookup_users(user_ids=idFollowersList[indxCtr[0]:indxCtr[1]])
                lookupFinalList = lookupFinalList + [user.id for user in lookupList if user.followers_count<400] 
            except tweepy.RateLimitError:   # Hit the ratelimit. Sleep for 15 minutes
                time.sleep(15*60)


        # Append the lookupFinalList to curr_id_list and write the 
        # current id and lookupFinalList to the file
        curr_id_list = curr_id_list + lookupFinalList
        fileDescriptor.write(str(curr_id) + " " + str(lookupFinalList) + '\n')
        del lookupFinalList[:]
    
except ValueError:
    print "Enter a valid id"
except IOError:
    print "I/O Error"
except IndexError:
    if len(curr_id_list) == 0:
        print "Retrieved the list of followers..."