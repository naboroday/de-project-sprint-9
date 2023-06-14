import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect
from lib.redis import RedisClient
from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor

app = Flask(__name__)


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)
    config = AppConfig()
    
    # Инициализация необходимых зависимостей
    consumer = KafkaConsumer()
    producer = KafkaProducer()
    redis = RedisClient()
    stg_repository = StgRepository(PgConnect())
    batch_size = 100
    
    proc = StgMessageProcessor(consumer, producer, redis, stg_repository, batch_size, app.logger)
    
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()
    
    app.run(debug=True, host='0.0.0.0', use_reloader=False)