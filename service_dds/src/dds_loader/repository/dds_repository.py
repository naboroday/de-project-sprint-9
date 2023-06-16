import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel
import hashlib
from decimal import Decimal

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

        result=self._execute_query(query, params)
        inserted_h_user = result.fetchone()[0]  # Получить вставленное значение h_user_pk

        return inserted_h_user

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

        result=self._execute_query(query, params)
        inserted_h_product_pk = result.fetchone()[0]  # Получить вставленное значение h_product_pk

        return inserted_h_product_pk

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

        result=self._execute_query(query, params)
        inserted_h_category_pk = result.fetchone()[0]  # Получить вставленное значение h_category_pk

        return inserted_h_category_pk

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

        result=self._execute_query(query, params)
        inserted_h_restaurant_pk = result.fetchone()[0]  # Получить вставленное значение h_restaurant_pk

        return inserted_h_restaurant_pk

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

        result=self._execute_query(query, params)
        inserted_h_order_pk = result.fetchone()[0]  # Получить вставленное значение h_order_pk

        return inserted_h_order_pk

    
    def insert_l_order_product(self, h_order_pk: uuid.UUID, h_product_pk: uuid.UUID, load_src: str) -> None:
        query = """
            INSERT INTO dds.l_order_product (h_order_pk, h_product_pk, load_src)
            VALUES (%(h_order_pk)s, %(h_product_pk)s, %(load_src)s)
            ON CONFLICT DO NOTHING
        """
        hk_order_product_pk = uuid.uuid4()

        params = {
            'hk_order_product_pk': hk_order_product_pk,
            'h_order_pk': h_order_pk,
            'h_product_pk': h_product_pk,
            'load_src': load_src
        }

        self._execute_query(query, params)


    def insert_l_product_restaurant(self, h_restaurant_pk: uuid.UUID, h_product_pk: uuid.UUID, load_src: str) -> None:
        query = """
            INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_src)
            VALUES (%(hk_product_restaurant_pk)s, %(h_restaurant_pk)s, %(h_product_pk)s, %(load_src)s)
            ON CONFLICT DO NOTHING
        """
        hk_product_restaurant_pk = uuid.uuid4()

        params = {
            'hk_product_restaurant_pk': hk_product_restaurant_pk,
            'h_restaurant_pk': h_restaurant_pk,
            'h_product_pk': h_product_pk,
            'load_src': load_src
        }

        self._execute_query(query, params)        

    def insert_l_product_category(self, h_category_pk: uuid.UUID, h_product_pk: uuid.UUID, load_src: str) -> None:
        query = """
            INSERT INTO dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_src)
            VALUES (%(hk_product_category_pk)s, %(h_category_pk)s, %(h_product_pk)s, %(load_src)s)
            ON CONFLICT DO NOTHING
        """
        hk_product_category_pk = uuid.uuid4()

        params = {
            'hk_product_category_pk': hk_product_category_pk,
            'h_category_pk': h_category_pk,
            'h_product_pk': h_product_pk,
            'load_src': load_src
        }

        self._execute_query(query, params)


    def insert_l_order_user(self, h_order_pk: uuid.UUID, h_user_pk: uuid.UUID, load_src: str) -> None:
        query = """
            INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_src)
            VALUES (%(hk_order_user_pk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_src)s)
            ON CONFLICT DO NOTHING
        """
        hk_order_user_pk = uuid.uuid4()

        params = {
            'hk_order_user_pk': hk_order_user_pk,
            'h_order_pk': h_order_pk,
            'h_user_pk': h_user_pk,
            'load_src': load_src
        }

        self._execute_query(query, params)

 
    def insert_s_user_names(self, h_user_pk: uuid.UUID, username: str, userlogin: str, load_src: str) -> None:
        query = """
            INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
            VALUES (%(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s, %(hk_user_names_hashdiff)s)
            ON CONFLICT DO NOTHING
        """
        load_dt = datetime.now()

        # Преобразование строки username в UUID с использованием хеша
        name_hash = hashlib.sha1(username.encode('utf-8')).hexdigest()
        hk_user_names_hashdiff = uuid.UUID(name_hash)

        params = {
            'h_user_pk': h_user_pk,
            'username': username,
            'userlogin': userlogin,
            'load_dt': load_dt,
            'load_src': load_src,
            'hk_user_names_hashdiff': hk_user_names_hashdiff
        }

        self._execute_query(query, params)   


    def insert_s_product_names(self, h_product_pk: uuid.UUID, name: str, load_src: str) -> None:
        query = """
            INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
            VALUES (%(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_product_names_hashdiff)s)
            ON CONFLICT DO NOTHING
        """
        load_dt = datetime.now()

        # Преобразование строки username в UUID с использованием хеша
        name_hash = hashlib.sha1(name.encode('utf-8')).hexdigest()
        hk_product_names_hashdiff = uuid.UUID(name_hash)

        params = {
            'h_product_pk': h_product_pk,
            'name': name,
            'load_dt': load_dt,
            'load_src': load_src,
            'hk_product_names_hashdiff': hk_product_names_hashdiff
        }

        self._execute_query(query, params)          

    def insert_s_restaurant_names(self, h_restaurant_pk: uuid.UUID, name: str, load_src: str) -> None:
        query = """
            INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src)
            VALUES (%(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_restaurant_names_hashdiff)s)
            ON CONFLICT DO NOTHING
        """
        load_dt = datetime.now()

        # Преобразование строки username в UUID с использованием хеша
        name_hash = hashlib.sha1(name.encode('utf-8')).hexdigest()
        hk_restaurant_names_hashdiff = uuid.UUID(name_hash)

        params = {
            'h_restaurant_pk': h_restaurant_pk,
            'name': name,
            'load_dt': load_dt,
            'load_src': load_src,
            'hk_restaurant_names_hashdiff': hk_restaurant_names_hashdiff
        }

        self._execute_query(query, params)

    def insert_s_order_cost(self, h_order_pk: uuid.UUID, cost: Decimal, payment: Decimal, load_src: str) -> None:
        query = """
            INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
            VALUES (%(h_order_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s, %(hk_order_cost_hashdiff)s)
            ON CONFLICT DO NOTHING
        """
        load_dt = datetime.now()

        # Преобразование строки username в UUID с использованием хеша
        name_hash = hashlib.sha1(cost.encode('utf-8')).hexdigest()
        hk_order_cost_hashdiff = uuid.UUID(name_hash)

        params = {
            'h_order_pk': h_order_pk,
            'cost': cost,
            'payment': payment,
            'load_dt': load_dt,
            'load_src': load_src,
            'hk_order_cost_hashdiff': hk_order_cost_hashdiff
        }

        self._execute_query(query, params)       

    def insert_s_order_status(self, h_order_pk: uuid.UUID, status: str, load_src: str) -> None:
        query = """
            INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
            VALUES (%(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s, %(hk_order_status_hashdiff)s)
            ON CONFLICT DO NOTHING
        """
        load_dt = datetime.now()

        # Преобразование строки username в UUID с использованием хеша
        name_hash = hashlib.sha1(status.encode('utf-8')).hexdigest()
        hk_order_status_hashdiff = uuid.UUID(name_hash)

        params = {
            'h_order_pk': h_order_pk,
            'status': status,
            'load_dt': load_dt,
            'load_src': load_src,
            'hk_order_status_hashdiff': hk_order_status_hashdiff
        }

        self._execute_query(query, params)        

    def _execute_query(self, query: str, params: Dict[str, Any]) -> None:
        self._db.execute(query, params)
