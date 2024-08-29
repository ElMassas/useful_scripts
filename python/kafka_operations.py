import os
import traceback
from typing import Any, Dict, Union

from confluent_kafka import Consumer, KafkaException, Message, Producer
from loguru import logger


def kafka_init(type: str, conf: Dict[str, Any]) -> Union[Consumer, Producer]:
    if type == "consumer":
        return init_kafka_consumer(conf)
    elif type == "producer":
        return init_kafka_producer(conf)
    else:
        raise ValueError("Type must be either 'consumer' or 'producer'")


def init_kafka_consumer(config: Dict[str, Any]) -> Consumer:
    try:
        consumer = Consumer(**config)
        consumer.subscribe([os.getenv("KAFKA_TOPIC", "")])
        logger.info(
            "[KAFKA CONSUMER] Consumer initialized and subscribed.",
            fields={
                "group.id": config["group.id"],
                "cluster": "wosa-prd",
                "log_level": 4,
            },
        )
        return consumer
    except Exception as err:
        logger.critical(
            "[KAFKA] Unable to initiate kafka consumer",
            fields={
                "exception_class": f"{str(err.__class__.__name__)}",
                "traceback": f"{traceback.format_exc()}",
                "error": f"{str(err)}",
            },
        )
        raise err


def init_kafka_producer(config: Dict[str, Any]) -> Producer:
    try:
        producer = Producer(**config)
        logger.info(
            "[KAFKA PRODUCER] Producer initialized.",
            fields={
                "username": config["group.id"],
                "cluster": "wosa-prd",
                "log_level": 4,
            },
        )
        return producer
    except Exception as err:
        logger.critical(
            "[KAFKA] Unable to initiate kafka producer",
            fields={
                "class": f"{str(err.__class__.__name__)}",
                "traceback": f"{traceback.format_exc()}",
                "str": f"{str(err)}",
            },
        )
        raise err


def shutdown_consumer(consumer: Consumer) -> bool:
    logger.info("Shutting down consumer.")
    try:
        consumer.close()
        return True
    except KafkaException as err:
        logger.error(
            "Failed to shut down consumer.",
            fields={
                "class": f"{str(err.__class__.__name__)}",
                "traceback": f"{traceback.format_exc()}",
                "str": f"{str(err)}",
            },
        )
        return False


def shutdown_producer(producer: Producer) -> bool:
    logger.info("Shutting down consumer.")
    try:
        producer.close()
        return True
    except KafkaException as err:
        logger.error(
            "Failed to shut down consumer.",
            fields={
                "class": f"{str(err.__class__.__name__)}",
                "traceback": f"{traceback.format_exc()}",
                "str": f"{str(err)}",
            },
        )
        return False


def delivery_callback(err: Any | None, msg: Message) -> bool:
    if err is not None:
        logger.debug(
            "[KAFKA PRODUCER] Message delivery failed",
            fields={
                "class": f"{str(err.__class__.__name__)}",
                "traceback": f"{traceback.format_exc()}",
                "str": f"{str(err)}",
            },
        )
        return False
    else:
        logger.debug(
            "[KAFKA PRODUCER] Message delivered to kafka",
            fields={"topic": msg.topic(), "partition": msg.partition()},
        )
        return True


def commit_message(
    consumer: Consumer, msg: Any, status: str, context: Dict[str, Any]
) -> bool:
    try:
        consumer.commit(message=msg, asynchronous=False)
        logger.info(
            "[KAFKA CONSUMER] Message committed!",
            fields={"status": status, "context": context},
        )
        return True
    except KafkaException as err:
        logger.error(
            "[KAFKA CONSUMER] Commit failed due to KafkaException.",
            fields={"status": status, "context": context, "error": str(err)},
        )
        return False
    except Exception as err:
        logger.error(
            "[KAFKA CONSUMER] Unexpected error during commit.",
            fields={"status": status, "context": context, "error": str(err)},
        )
        return False


def produce_message(msg: str, producer: Producer, topic: str) -> bool:
    logger.debug(
        "[KAFKA PRODUCER] Producing message",
        fields={"topic": topic, "msg_type": str(type(msg))},
    )
    try:
        producer.produce(
            topic,
            value=msg,
            callback=delivery_callback,
        )
        producer.poll(1)
        return True
    except BufferError:
        logger.warning(
            "[KAFKA PRODUCER] Local producer queue is full. Messages awaiting delivery",
            fields={"queue_size": f"{len(producer)}", "topic": topic},
        )
        producer.flush()
        return False
    except KafkaException as err:
        logger.exception(
            "[KAFKA PRODUCER] Exception producing to kafka!",
            fields={
                "class": f"{str(err.__class__.__name__)}",
                "traceback": f"{traceback.format_exc()}",
                "str": f"{str(err)}",
            },
        )
        return False
    except Exception as err:
        logger.exception(
            "[KAFKA PRODUCER] Unknown error",
            fields={
                "class": f"{str(err.__class__.__name__)}",
                "traceback": f"{traceback.format_exc()}",
                "str": f"{str(err)}",
            },
        )
        return False
