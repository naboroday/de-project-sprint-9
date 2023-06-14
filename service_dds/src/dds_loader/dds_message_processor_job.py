import time
import json
from datetime import datetime
from logging import Logger

from lib.redis.redis_client import RedisClient
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._dds_repository = dds_repository
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

            self._dds_repository.insert_h_order(
                order_id=message.get('object_id'),
                order_dt=datetime.strptime(payload['date'], '%Y-%m-%d %H:%M:%S'),
                load_src='stg_message_processor'
            )

            self._dds_repository.insert_h_user(
                user_id=user_id,
                load_src='orders-system-kafka'
            )

            self._dds_repository.insert_h_restaurant(
                restaurant_id=restaurant_id,
                load_src='orders-system-kafka'
            )

            for product in products:
                category_name = product['category']
                self._dds_repository.insert_h_category(
                    category_name=category_name,
                    load_src='orders-system-kafka'
                )

                self._dds_repository.insert_h_product(
                    product_id=product['id'],
                    load_src='orders-system-kafka'
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