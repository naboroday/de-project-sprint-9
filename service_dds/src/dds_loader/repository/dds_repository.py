import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_h_user(self, user_id: str, load_src: str) -> None:
        query = """
            INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
            VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
        """
        h_user_pk = uuid.uuid4()
        load_dt = datetime.now()

        params = {
            'h_user_pk': h_user_pk,
            'user_id': user_id,
            'load_dt': load_dt,
            'load_src': load_src
        }

        self._execute_query(query, params)

    def insert_h_product(self, product_id: str, load_src: str) -> None:
        query = """
            INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
            VALUES (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
        """
        h_product_pk = uuid.uuid4()
        load_dt = datetime.now()

        params = {
            'h_product_pk': h_product_pk,
            'product_id': product_id,
            'load_dt': load_dt,
            'load_src': load_src
        }

        self._execute_query(query, params)

    def insert_h_category(self, category_name: str, load_src: str) -> None:
        query = """
            INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
            VALUES (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
        """
        h_category_pk = uuid.uuid4()
        load_dt = datetime.now()

        params = {
            'h_category_pk': h_category_pk,
            'category_name': category_name,
            'load_dt': load_dt,
            'load_src': load_src
        }

        self._execute_query(query, params)

    def insert_h_restaurant(self, restaurant_id: str, load_src: str) -> None:
        query = """
            INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
            VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
        """
        h_restaurant_pk = uuid.uuid4()
        load_dt = datetime.now()

        params = {
            'h_restaurant_pk': h_restaurant_pk,
            'restaurant_id': restaurant_id,
            'load_dt': load_dt,
            'load_src': load_src
        }

        self._execute_query(query, params)

    def insert_h_order(self, order_id: int, order_dt: datetime, load_src: str) -> None:
        query = """
            INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
            VALUES (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
        """
        h_order_pk = uuid.uuid4()
        load_dt = datetime.now()

        params = {
            'h_order_pk': h_order_pk,
            'order_id': order_id,
            'order_dt': order_dt,
            'load_dt': load_dt,
            'load_src': load_src
        }

        self._execute_query(query, params)

    def _execute_query(self, query: str, params: Dict[str, Any]) -> None:
        self._db.execute(query, params)