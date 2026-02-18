from datetime import datetime
from logging import Logger

from app_config import AppConfig
from cdm_loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 logger: Logger,
                 ) -> None:

        self._logger = logger
        self._batch_size = 100
        self._config = AppConfig()
        self._consumer = self._config.kafka_consumer()
        self._repo = CdmRepository(self._config.pg_warehouse_db())

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        # Ensure schema/tables exist even if there are no new messages yet.
        try:
            self._repo.ensure_ddl()
        except Exception:
            self._logger.exception("Failed to ensure CDM tables")
            self._logger.info(f"{datetime.utcnow()}: FINISH")
            return

        processed = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume_raw(timeout=3.0)
            if msg is None:
                break

            payload = self._consumer.decode(msg)
            try:
                self._repo.process_message(payload)
            except ValueError as e:
                self._logger.error(f"Skip message (bad format): {e}")
                self._consumer.commit(msg)
                continue
            except Exception:
                self._logger.exception("Failed to process message; will retry later")
                break

            self._consumer.commit(msg)
            processed += 1

        self._logger.info(f"{datetime.utcnow()}: processed={processed}")
        self._logger.info(f"{datetime.utcnow()}: FINISH")
