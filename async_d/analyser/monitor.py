import time
import asyncio
import logging
from datetime import timedelta

from .analyser import Analyser
from .singleton_meta import SingletonMeta

logger = logging.getLogger(__name__)


class Monitor(metaclass=SingletonMeta):
    def __init__(self, report_interval=10) -> None:
        self.registered_analysers = []
        self.start_time = None
        self.report_interval = report_interval
        self._monitor_task = None

    def start(self):
        for analyser in self.registered_analysers:
            analyser.start()
        if self._monitor_task is None:
            self._monitor_task = asyncio.create_task(self.get_all_info())
        self.start_time = time.monotonic()

    async def get_all_info(self):
        while True:
            await asyncio.sleep(self.report_interval)
            total_time = time.monotonic() - self.start_time
            readable_time = str(timedelta(seconds=total_time))
            logger.info("=" * 80 + f"\nTotal execution time: {readable_time}")
            for analyser in self.registered_analysers:
                string = analyser.report()
                logger.info("=" * 80 + "\n" + string)

    def register(self, *analysers):
        for analyser in analysers:
            assert isinstance(analyser, Analyser)
            self.registered_analysers.append(analyser)
