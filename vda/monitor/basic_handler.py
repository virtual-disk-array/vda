from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from threading import Thread
from collections import namedtuple
import random

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vda.common.constant import Constant
from vda.common.utils import AtomicCounter, Histogram


logger = logging.getLogger(__name__)


HandlerMetricConf = namedtuple("HandlerMetricConf", [
    "backlog_min_val",
    "backlog_width",
    "backlog_steps",
    "process_min_val",
    "process_width",
    "process_steps",
])


HandlerMetric = namedtuple("HandlerMetric", [
    "submit_cnt",
    "process_success_cnt",
    "process_failure_cnt",
    "backlog_success_cnt",
    "backlog_failure_cnt",
    "loop_cnt",
    "intime_cnt",
    "overtime_cnt",
    "process_histogram",
    "process_duration_sum",
    "backlog_histogram",
    "backlog_duration_sum",
])


class BasicHandler(ABC):

    def __init__(
            self, db_uri, db_kwargs, total, current,
            interval, max_workers, handler_metric_conf):
        logger.info(
            "%s db_kwargs=%s total=%d current=%d interval=%d max_workers=%d",
            self.__class__.__name__, db_kwargs, total,
            current, interval, max_workers,
        )
        logger.info(
            "%s handler_metric_conf=%s",
            self.__class__.__name__,
            handler_metric_conf,
        )
        step = (Constant.MAX_HASH_CODE + total - 1) // total
        begin = step * current
        end = begin + step
        if end > Constant.MAX_HASH_CODE:
            end = Constant.MAX_HASH_CODE
        size = end - begin
        logger.info(
            "%s begin=%d end=%d size=%d",
            self.__class__.__name__, begin, end, size,
        )
        self.begin = begin
        self.end = end
        self.engine = create_engine(db_uri, **db_kwargs)
        self.Session = sessionmaker(bind=self.engine)
        self.interval = interval
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.submit_counter = AtomicCounter()
        self.process_success_counter = AtomicCounter()
        self.process_failure_counter = AtomicCounter()
        self.backlog_success_cnt = 0
        self.backlog_failure_cnt = 0
        self.loop_cnt = 0
        self.intime_cnt = 0
        self.overtime_cnt = 0
        self.backlog_histogram = Histogram(
            min_val=handler_metric_conf.backlog_min_val,
            width=handler_metric_conf.backlog_width,
            steps=handler_metric_conf.backlog_steps,
        )
        self.backlog_duration_counter = AtomicCounter()
        self.process_histogram = Histogram(
            min_val=handler_metric_conf.process_min_val,
            width=handler_metric_conf.process_width,
            steps=handler_metric_conf.process_steps,
        )
        self.process_duration_counter = AtomicCounter()

    @abstractmethod
    def get_backlog_list(self, session, begin, end):
        raise NotImplementedError

    @abstractmethod
    def process_backlog(self, session, backlog):
        raise NotImplementedError

    def process(self, backlog):
        session = self.Session()
        try:
            logger.info(
                "%s backlog: %s",
                self.__class__.__name__,
                backlog,
            )
            t1 = time.time()
            self.process_backlog(session, backlog)
            duration = int((time.time() - t1) * 1000000)
            self.process_histogram.update(duration)
            self.process_duration_counter.add(duration)
        except Exception:
            logger.error(
                "%s process_backlog failed",
                self.__class__.__name__,
                exc_info=True,
            )
            session.rollback()
            self.process_failure_counter.inc()
        else:
            logger.info(
                "%s process_backlog finished",
                self.__class__.__name__,
            )
            self.process_success_counter.inc()
        finally:
            session.close()

    def run(self):
        # add a random dely
        # avoid all instances query at the same time
        delay_ms = random.randint(0, 1000 * self.interval)
        delay = delay_ms / 1000
        time.sleep(delay)
        while True:
            start_time = time.time()
            self.loop_cnt += 1
            session = self.Session()
            try:
                logger.info("%s get_backlog_list", self.__class__.__name__)
                t1 = time.time()
                backlog_list = self.get_backlog_list(
                    session, self.begin, self.end)
                duration = int((time.time() - t1) * 1000000)
                self.backlog_histogram.update(duration)
                self.backlog_duration_counter.add(duration)
                logger.info(
                    "%s get_backlog_list result: %s",
                    self.__class__.__name__,
                    backlog_list,
                )
            except Exception:
                logger.error(
                    "%s get_backlog_list failed",
                    self.__class__.__name__,
                    exc_info=True,
                )
                session.rollback()
                self.backlog_failure_cnt += 1
            else:
                logger.info(
                    "%s backlog_list length: %d",
                    self.__class__.__name__,
                    len(backlog_list),
                )
                self.backlog_success_cnt += 1
                for backlog in backlog_list:
                    self.submit_counter.inc()
                    self.executor.submit(self.process, backlog)
            finally:
                session.close()

            delta = time.time() - start_time
            if delta > self.interval:
                logger.warning("%s overtime", self.__class__.__name__)
                self.overtime_cnt += 1
            else:
                self.intime_cnt += 1
                time.sleep(self.interval - delta)

    def start(self):
        t = Thread(target=self.run)
        t.start()

    def get_handler_metric(self):
        handler_metric = HandlerMetric(
            submit_cnt=self.submit_counter.get(),
            process_success_cnt=self.process_success_counter.get(),
            process_failure_cnt=self.process_failure_counter.get(),
            backlog_success_cnt=self.backlog_success_cnt,
            backlog_failure_cnt=self.backlog_failure_cnt,
            loop_cnt=self.loop_cnt,
            intime_cnt=self.intime_cnt,
            overtime_cnt=self.overtime_cnt,
            process_histogram=self.process_histogram.get(),
            process_duration_sum=self.process_duration_counter.get(),
            backlog_histogram=self.backlog_histogram.get(),
            backlog_duration_sum=self.backlog_duration_counter.get(),
        )
        return handler_metric
