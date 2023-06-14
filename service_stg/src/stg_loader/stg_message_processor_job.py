import time
import json
from datetime import datetime
from logging import Logger

from lib.redis.redis_client import RedisClient
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from stg_loader.repository.stg_repository import StgRepository



class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient, 
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger


    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            message = self._consumer.consume()

            if message is None:
                break

            payload = message.get('payload')
            user_id = payload['user']['id']
            restaurant_id = payload['restaurant']['id']
            products = payload['order_items']

            user_info = self._redis.get(user_id)
            restaurant_info = self._redis.get(restaurant_id)


            self._stg_repository.order_events_insert(
                object_id=message.get('object_id'),
                object_type=message.get('object_type'),
                sent_dttm=datetime.utcnow(),
                payload=payload
            )

            output_message = {
                'object_id': message.get('object_id'),
                'object_type': message.get('object_type'),
                'payload': {
                    'id': message.get('object_id'),
                    'date': payload['date'],
                    'cost': payload['cost'],
                    'payment': payload['payment'],
                    'status': payload['final_status'],
                    'restaurant': {
                        'id': restaurant_id,
                        'name': restaurant_info['name']
                    },
                    'user': {
                        'id': user_id,
                        'name': user_info['name']
                    },
                    'products': [
                        {
                            'id': product['id'],
                            'price': product['price'],
                            'quantity': product['quantity'],
                            'name': product['name'],
                            'category': product['category']
                        } for product in products
                    ],
                'user_id': user_id
                }
            }
            self._producer.produce(output_message)

        self._logger.info(f"{datetime.utcnow()}: FINISH")