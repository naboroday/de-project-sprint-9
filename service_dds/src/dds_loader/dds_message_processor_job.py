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
            user_name = payload['user']['name']
            restaurant_id = payload['restaurant']['id']
            restaurant_name = payload['restaurant']['name']
            products = payload['order_items']
            cost = payload['cost']
            payment = payload['payment']
            status = payload['status']


            insert_h_order=self._dds_repository.insert_h_order(
                order_id=message.get('object_id'),
                order_dt=datetime.strptime(payload['date'], '%Y-%m-%d %H:%M:%S'),
                load_src='stg_message_processor'
            )

            insert_h_user=self._dds_repository.insert_h_user(
                user_id=user_id,
                load_src='orders-system-kafka'
            )

            insert_h_restaurant=self._dds_repository.insert_h_restaurant(
                restaurant_id=restaurant_id,
                load_src='orders-system-kafka'
            )


            #Хабы


            self._dds_repository.insert_l_order_user(
                h_order_pk=insert_h_order,
                h_user_pk=insert_h_user,
                load_src='orders-system-kafka'
            )

            self._dds_repository.insert_s_user_names(
                h_user_pk= insert_h_user,
                username= user_name,
                userlogin= user_id,
                load_src= 'orders-system-kafka'
            )            

            self._dds_repository.insert_s_restaurant_names(
                h_restaurant_pk= insert_h_restaurant,
                name= restaurant_name,
                load_src= 'orders-system-kafka'
            ) 

            self._dds_repository.insert_s_order_cost(
                h_order_pk= insert_h_order,
                cost= cost,
                payment=payment,
                load_src= 'orders-system-kafka'
            ) 

            self._dds_repository.insert_insert_s_order_statuss_order_cost(
                h_order_pk= insert_h_order,
                status= status,
                load_src= 'orders-system-kafka'
            ) 

            for product in products:
                category_name = product['category']
                insert_h_category=self._dds_repository.insert_h_category(
                    category_name=category_name,
                    load_src='orders-system-kafka'
                )

                insert_h_product=self._dds_repository.insert_h_product(
                    product_id=product['id'],
                    load_src='orders-system-kafka'
                )

                #Хабы

                self._dds_repository.insert_l_order_product(
                    h_order_pk=insert_h_order,
                    h_product_pk=insert_h_product,
                    load_src='orders-system-kafka'
                )

                self._dds_repository.insert_l_product_restaurant(
                    h_restaurant_pk=insert_h_restaurant,
                    h_product_pk=insert_h_product,
                    load_src='orders-system-kafka'
                )     

                self._dds_repository.insert_l_product_category(
                    h_category_pk=insert_h_category,
                    h_product_pk=insert_h_product,
                    load_src='orders-system-kafka'
                )            

                self._dds_repository.insert_s_product_names(
                    h_product_pk= insert_h_product,
                    name= product['name'],
                    load_src= 'orders-system-kafka'
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
                        'id': restaurant_id
                    },
                    'user': {
                        'id': user_id
                    },
                    'products': [
                        {
                            'id': product['id'],
                            'price': product['price'],
                            'quantity': product['quantity'],
                            'name': product['name'],
                            'category': product['category']
                        } for product in products
                    ]
                }
            }
            self._producer.produce(output_message)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
