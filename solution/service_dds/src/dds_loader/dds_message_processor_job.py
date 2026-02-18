from datetime import datetime
from logging import Logger

from app_config import AppConfig
from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 logger: Logger) -> None:

        self._logger = logger

        self._batch_size = 30
        self._config = AppConfig()
        self._consumer = self._config.kafka_consumer()
        self._producer = self._config.kafka_producer()
        self._repo = DdsRepository(self._config.pg_warehouse_db())

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        # Ensure schema/tables exist even if there are no new messages yet.
        try:
            self._repo.ensure_ddl()
        except Exception:
            self._logger.exception("Failed to ensure DDS tables")
            self._logger.info(f"{datetime.utcnow()}: FINISH")
            return

        processed = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume_raw(timeout=3.0)
            if msg is None:
                break

            payload = self._consumer.decode(msg)
            try:
                out = self._repo.process_order(payload)
            except ValueError as e:
                # Poison / unexpected message format: log and skip to avoid blocking the consumer group.
                self._logger.error(f"Skip message (bad format): {e}")
                self._consumer.commit(msg)
                continue

            try:
                self._producer.produce(out)
                self._consumer.commit(msg)
                processed += 1
            except Exception:
                # Do not commit on failure so the message can be retried.
                self._logger.exception("Failed to persist/produce message; will retry later")
                break

        self._logger.info(f"{datetime.utcnow()}: processed={processed}")
        self._logger.info(f"{datetime.utcnow()}: FINISH")
