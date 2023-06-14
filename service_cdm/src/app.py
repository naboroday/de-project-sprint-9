import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from lib.redis.redis_client import RedisClient
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor

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
    
    proc = CdmMessageProcessor(consumer, producer, redis, stg_repository, batch_size, app.logger)
    
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()
    
    app.run(debug=True, host='0.0.0.0', use_reloader=False)