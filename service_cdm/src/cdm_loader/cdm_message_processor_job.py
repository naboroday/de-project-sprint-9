import uuid
from datetime import datetime
from logging import Logger
from lib.pg import PgConnect
from typing import Any, Dict
from cdm_loader.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self, logger: Logger, cdm_repository: CdmRepository) -> None:
        self._logger = logger
        self._batch_size = 30
        self._cdm_repository = cdm_repository

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

            # Обработка сообщения и вставка в CDM
            for product in products:
                product_id = product['id']
                product_name = product['name']
                category_id = product['category']['id']
                category_name = product['category']['name']

                self._cdm_repository.insert_user_product_counter(user_id, product_id, product_name)
                self._cdm_repository.insert_user_category_counter(user_id, category_id, category_name)

        self._logger.info(f"{datetime.utcnow()}: FINISH")