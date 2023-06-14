import uuid
from typing import Any, Dict
from datetime import datetime
from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_user_product_counter(self, user_id: str, product_id: str, product_name: str) -> None:
        query = """
            INSERT INTO cdm.user_product_counters (id, user_id, product_id, product_name, order_cnt)
            VALUES (%(id)s, %(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)
            ON CONFLICT (user_id, product_id)
            DO UPDATE SET order_cnt = cdm.user_product_counters.order_cnt + 1
        """
        id = uuid.uuid4()

        params = {
            'id': id,
            'user_id': user_id,
            'product_id': product_id,
            'product_name': product_name,
            'order_cnt': 1
        }

        self._execute_query(query, params)

    def insert_user_category_counter(self, user_id: str, category_id: str, category_name: str) -> None:
        query = """
            INSERT INTO cdm.user_category_counters (id, user_id, category_id, category_name, order_cnt)
            VALUES (%(id)s, %(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s)
            ON CONFLICT (user_id, category_id)
            DO UPDATE SET order_cnt = cdm.user_category_counters.order_cnt + 1
        """
        id = uuid.uuid4()

        params = {
            'id': id,
            'user_id': user_id,
            'category_id': category_id,
            'category_name': category_name,
            'order_cnt': 1
        }

        self._execute_query(query, params)

 
    def _execute_query(self, query: str, params: Dict[str, Any]) -> None:
        self._db.execute(query, params)