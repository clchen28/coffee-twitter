import os
import tweepy
import googlemaps
from dotenv import load_dotenv, find_dotenv
from kafka import KafkaProducer

# https://gist.github.com/dev-techmoe/ef676cdd03ac47ac503e856282077bf2
# https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
# Structure of Status object

load_dotenv(find_dotenv())

def center_bounding_box(bounding_box):
    # Need corners if bounding box is a Polygon
    if len(bounding_box.coordinates[0]) == 4:
        point1 = bounding_box.coordinates[0][0]
        point2 = bounding_box.coordinates[0][1]
        point3 = bounding_box.coordinates[0][2]
        point4 = bounding_box.coordinates[0][3]
        lng = (point1[0] + point2[0] + point3[0] + point4[0]) / 4.0
        lat = (point1[1] + point2[1] + point3[1] + point4[1]) / 4.0
        return [lat, lng]
    # Need just the point if bounding box is a Point
    # TODO: See if there is any situation where this is the case
    """
    elif len(bounding_box.coordinates[0]) == 1: 
        lng = bounding_box.coordinates[0][0]
        lat = bounding_box.coordinates[0][1]
    """
    return None

class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
            value_serializer=lambda v: v.encode('utf-8'))
    def on_status(self, status):
        lng = None
        lat = None
        gmap = googlemaps.Client(key=os.environ.get("GOOGLE_GEOCODE_API_KEY"))
        if status.coordinates:
            lng = status.coordinates["coordinates"][0]
            lat = status.coordinates["coordinates"][1]
        elif status.place:
            [lat, lng] = center_bounding_box(status.place.bounding_box)
        elif status.user.location:
            location = gmap.geocode(status.user.location)
            if location:
                location = location[0]["geometry"]["location"]
                lat, lng = location["lat"], location["lng"]
        if lat and lng:
            date = status.created_at.strftime("%Y-%m-%d %H:%M:%S")
            value = status.id_str + "," + date + "," + str(lat) + "," + str(lng)
            print(value)
            self.producer.send(topic="coffee-topic", value=value)
        

if __name__ == "__main__":
    auth = tweepy.OAuthHandler(os.environ.get("CONSUMER_KEY"),
                            os.environ.get("CONSUMER_SECRET"))
    auth.set_access_token(os.environ.get("ACCESS_TOKEN"),
                        os.environ.get("ACCESS_TOKEN_SECRET"))

    api = tweepy.API(auth)

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(track=['coffee'])
