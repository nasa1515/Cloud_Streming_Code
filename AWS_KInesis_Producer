# -*- coding: utf-8 -*-

import boto3
import time
import json

# key, secret, region_name 은 여러분의 환경에 맞는걸 넣어야 합니다.
client = boto3.client('kinesis', region_name='ap-northeast-1', aws_access_key_id='<>', aws_secret_access_key='<>')

def put_records(records):

    kinesis_records = []

    for r in records:
        kinesis_records.append(
            {
                    'Data': json.dumps(r).encode('utf-8'),
                    # 'ExplicitHashKey': 'string',
                    'PartitionKey': 'test'
            }
        )
    # Data 에는 바이너리 형식으로, ExplicitHashKey은 optional입니다. PartitionKey 를 기준으로 hash값을 얻고 이 값을 가지고
    # 실제 들어갈 shard 번호가 결정됩니다. 같은 PartitionKey를 넣으면 항상 같은 shard에만 들어갑니다.
    # 만약 round robin 방식으로 여러 shard에 골고루 넣고 싶으면 random string을 넣으세요.
##kinesis stream name 변경~~~~~~~~!!!!!!!!!!
    response = client.put_records(
        Records=kinesis_records,
        StreamName='testaa11'
    )

    return response


def main():
    while True:
        print('start to send')
        data = [
            {
                'time': time.time()
            },
            {
                'time': time.time()+10
            }
        ]
        response = put_records(data)
        print('response: {}'.format(response))

        time.sleep(10)


if __name__ == "__main__":
    # execute only if run as a script
    main()