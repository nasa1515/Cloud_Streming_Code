import json
from google.auth import jwt
from concurrent import futures
from google.cloud import pubsub_v1

service_account_info = json.load(open("/home/nasa1515/docker/producer/GCP/data-cloocus-ffd800735dd1.json"))
credentials_pub = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"

credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=credentials_pub
)

publisher = pubsub_v1.PublisherClient(credentials=credentials)


project_id = "data-cloocus"
topic_id = "pubsub_nasa1515"

topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 100):
    data_str = f"nasa1515_Pubsub_Massage : {n}"
    data = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(future.result())

print(f"Published messages to {topic_path}.")