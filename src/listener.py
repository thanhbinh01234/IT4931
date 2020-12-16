from tweepy.streaming import StreamListener
import json
import time
import socket


class listener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        # Read the tweet into a dictionary
        tweetdict = json.loads(data)

        try:
            username = tweetdict['user']['name'].encode('utf-8')
            text = tweetdict['text'].encode('utf-8')
            msg = '---'.encode('utf-8') + username + \
                '+++'.encode('utf-8') + text

            print(msg)
            self.client_socket.send(msg)
        except KeyError:
            print('Error.')

        return True

    def on_error(self, status):
        print(status)
