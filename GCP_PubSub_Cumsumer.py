import os
import json
from google.auth import jwt
from google.cloud import pubsub_v1


service_account_info = json.load(open("/home/nasa1515/docker/producer/GCP/data-cloocus-ffd800735dd1.json"))
credentials_sub = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=credentials_sub
)

subscriber = pubsub_v1.SubscriberClient(credentials=credentials)


project_id = "data-cloocus"
topic_id = "pubsub_nasa1515"
subscription = "pubsub_nasa1515-sub"
topic_name = f'projects/{project_id}/topics/{topic_id}'
subscription_name = f'projects/{project_id}/subscriptions/{subscription}'


def callback(message):
    print(message.data)
    message.ack()


with pubsub_v1.SubscriberClient() as subscriber:
    try:
        response = subscriber.get_subscription(subscription=subscription_name)
        print(response)
    except:
        subscriber.create_subscription(
        name=subscription_name, topic=topic_name)
        future = subscriber.subscribe(subscription_name, callback)
        try:
            future.result()
            print(future.result())
        except KeyboardInterrupt:
            future.cancel()
    else:
        future = subscriber.subscribe(subscription_name, callback)
        try:
            future.result()
        except KeyboardInterrupt:
            future.cancel()