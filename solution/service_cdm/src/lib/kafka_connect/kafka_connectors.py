import json
from typing import Dict, Optional

from confluent_kafka import Consumer, Message, Producer


def error_callback(err):
    print('Something went wrong: {}'.format(err))


class KafkaProducer:
    def __init__(self, host: str, port: int, user: str, password: str, topic: str, cert_path: str) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'error_cb': error_callback,
        }

        self.topic = topic
        self.p = Producer(params)

    def produce(self, payload: Dict) -> None:
        self.p.produce(self.topic, json.dumps(payload))
        self.p.flush(10)


class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,  # '',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'error_cb': error_callback,
            'client.id': 'someclientkey'
        }

        self.topic = topic
        self.c = Consumer(params)
        self.c.subscribe([topic])

    def _poll(self, timeout: float = 3.0) -> Optional[Message]:
        msg = self.c.poll(timeout=timeout)
        if not msg:
            return None
        if msg.error():
            raise Exception(msg.error())
        return msg

    @staticmethod
    def _decode(msg: Message) -> Dict:
        val = msg.value().decode()
        return json.loads(val)

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        msg = self._poll(timeout=timeout)
        if msg is None:
            return None
        return self._decode(msg)

    def consume_raw(self, timeout: float = 3.0) -> Optional[Message]:
        return self._poll(timeout=timeout)

    def decode(self, msg: Message) -> Dict:
        return self._decode(msg)

    def commit(self, msg: Message) -> None:
        # Synchronous commit: we only advance offsets after successful processing.
        self.c.commit(message=msg, asynchronous=False)

    def close(self) -> None:
        self.c.close()
