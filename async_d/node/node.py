import logging
import traceback
import functools
from typing import Iterable, AsyncIterable
import inspect

# import gevent
import copy
import asyncio
from asyncio import Queue, Semaphore
from asyncio import TimeoutError as AsyncTimeoutError
# from gevent import sleep, spawn
# from gevent.queue import Queue
# from gevent.queue import Empty
from tenacity import (
    retry,
    retry_if_not_exception_type,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from .decorator import *

from .. import ASYNC_D_CONFIG
from ..exceptions import NodeProcessingError, NodeStop
from .abstract_node import AbstractNode
from .node_link import NodeLink
logger = logging.getLogger(__name__)
DEBUG_MOD = ASYNC_D_CONFIG.get("debug_mode", False)


class Node(AbstractNode, NodeLink):
    def __init__(
        self,
        proc_func,
        worker_num=None,
        queue_size=None,
        no_input=False,
        is_data_iterable=False,
        no_output=False,
        discard_none_output=False,
        skip_error=True,
        timeout=None,
        put_deepcopy_data=False
    ) -> None:
        super().__init__()
        self.timeout = timeout if timeout else ASYNC_D_CONFIG.get("timeout", None)
        self.worker_num = (
            worker_num if worker_num else ASYNC_D_CONFIG.get("worker_num", 1)
        )
        self.queue_size = (
            queue_size if queue_size else ASYNC_D_CONFIG.get("queue_size", 1)
        )

        ## 留存最原始回调函数，防止多次装饰后地址发生变化
        self._orig_proc_func = proc_func
        orig = inspect.unwrap(proc_func)
        self.__name__ = f"{orig.__name__}_{hex(id(orig))}"
        self.head = self
        self.tail = self
        self.is_start = False

        self.src_queue = Queue(self.queue_size)
        self.criterias = {}
        self.src_nodes = {}
        self.dst_nodes = {}
        self.get_data_lock = Semaphore(1)

        self.no_input = no_input
        self.no_output = no_output
        self.is_data_iterable = is_data_iterable
        self.discard_none_output = discard_none_output
        self.skip_error = skip_error
        # 当多个下游共享同一数据或输出后可能被修改时，可开启 put_deepcopy_data=True，避免并发修改冲突。
        self.put_deepcopy_data = put_deepcopy_data
        
        # first decorator will first wrap, as the inner decorator
        self.get_decorators = []
        self.proc_decorators = []
        self.put_decorators = []
        self._proc_data = self._error_decorator(proc_func)

        self.tasks = []  # store all worker tasks
        self.executing_data_queue = []

    def start(self):
        """
        Starts the node's processing loop.
        """
        # 保证“无输出”节点没有下游，“有输出”节点至少一个下游。
        self._validate_destinations()
        self._setup_decorators()
        self._spawn_workers()
        self.is_start = True
        logger.info(
            f"Node {self.__name__} start, src_nodes: {self.src_nodes}, dst_nodes: {self.dst_nodes}"
        )
        return self.tasks

    async def end(self):
        """
        Signals the end of the pipeline by putting a stop flag in the source queue.
        """
        for _ in range(self.worker_num):
            await self.src_queue.put(NodeStop())

    async def put(self, data):
        await self.src_queue.put(data)

    def connect(self, node, criteria=None):
        self.set_dst_node(node)
        node.set_src_node(self)

        if criteria:
            self.set_dst_criteria(node, criteria)
        return node

    def set_dst_criteria(self, node, criteria):
        self.criterias[node.__name__] = criteria

    def set_dst_node(self, node):
        self.dst_nodes[node.__name__] = node

    def set_src_node(self, node):
        self.src_nodes[node.__name__] = node

    def add_proc_decorator(self, decorator):
        self.proc_decorators.append(decorator)

    def add_get_decorator(self, decorator):
        self.get_decorators.append(decorator)

    def add_put_decorator(self, decorator):
        self.put_decorators.append(decorator)

    def _validate_destinations(self):
        if self.no_output:
            assert (
                len(self.dst_nodes) == 0
            ), f"Node {self.__name__} has output queues, but set as no_output"
        else:
            assert len(self.dst_nodes) > 0, f"Node {self.__name__} dst_node is empty"

    ## 所有增加的装饰器需要满足同步和异步的装饰，利用inspect的功能进行判断
    def _setup_decorators(self):
        if self.skip_error:
            self.add_proc_decorator(skip_data_decorator)
        self._rearrange_proc_decorator()
        for decorator in self.get_decorators:
            self._get_one_data = decorator(self._get_one_data)
        for decorator in self.proc_decorators:
            self._proc_data = decorator(self._proc_data)
        for decorator in self.put_decorators:
            self._put_data = decorator(self._put_data)

    def _rearrange_proc_decorator(self):
        new_decorators = []

        def set_bottom_decorator(decorator):
            # former decorator in the inner layer
            if decorator in self.proc_decorators:
                new_decorators.append(decorator)
                self.proc_decorators.remove(decorator)

        def set_top_decorator(decorator):
            # latter decorator in the outer layer
            if decorator in self.proc_decorators:
                new_decorators.remove(decorator)
                new_decorators.append(decorator)

        for decorator in self.proc_decorators:
            new_decorators.append(decorator)

        set_top_decorator(label_proc_decorator)
        set_top_decorator(skip_data_decorator)

        self.proc_decorators = new_decorators

    def _spawn_workers(self):
        # 1. 重置旧任务，防止重复调用时累积
        self.tasks.clear()
        for i in range(self.worker_num):
            # task = spawn(self._func_wrapper, i)
            task = asyncio.create_task(self._func_wrapper(i))
            self.tasks.append(task)

        self.get_data_generator = self._get_data()
        self.is_start = True
        return self.tasks

    async def _func_wrapper(self, task_id):
        """
        Wraps the processing function and handles concurrency.
        """
        logger.debug(f"Name: {self.__name__}, id: {task_id} start")
        while self.is_start:
            data = None
            try:
                if not self.no_input:
                    async with self.get_data_lock:
                        data = await self.get_data_generator.__anext__()
                    if isinstance(data, NodeStop):
                        raise NodeStop()
                    self.executing_data_queue.append(data)
                result = self._proc_data(data)
                if inspect.isawaitable(result):
                    result = await result
                await self._put_data(result)
            except NodeStop:
                await self._put_data(NodeStop())
                logger.info(f"Node {self.__name__} No. {task_id} stop")
                break
            finally:
                if data in self.executing_data_queue:
                    self.executing_data_queue.remove(data)
            if task_id == 0 and self._is_upstream_end():
                logger.info(f"Node {self.__name__} No. {task_id} upstream end")
                await self.end()
                break
            await asyncio.sleep(0)
        if all(task.done() for task in self.tasks if task is not asyncio.current_task()):
            logger.info(f"Node {self.__name__} No. {task_id} all other tasks finished")
            self.is_start = False
            
    async def _get_data(self):
        while self.is_start:
            try:
                data = await asyncio.wait_for(self.src_queue.get(), timeout=0.1)
            except AsyncTimeoutError:
                await asyncio.sleep(0.01)
                continue
            # 拆分 sync/async 两种可迭代
            async for d in self._get_one_data(data):
                yield d

        yield NodeStop()

    async def _get_one_data(self, data):
        """
        支持：
          1) data 是 NodeStop
          2) 同步可迭代（list/tuple/...）
          3) 异步可迭代（async generator）
          4) 普通单条 data
        """
        # 1) Stop 信号
        if isinstance(data, NodeStop):
            yield NodeStop()
            return
        
         # 2) 批量拆分
        if self.is_data_iterable:
            # 2a) 异步可迭代
            if isinstance(data, AsyncIterable):
                async for item in data:
                    yield item
                return
            # 2b) 同步可迭代
            assert isinstance(data, Iterable), (
                f"Unpack decorator only supports iterable data, current node: {self.__name__}, current data: {data}"
            )
            for item in data:
                yield item
            return
        # 3) 单条输出
        else:
            yield data
        # if self.is_data_iterable:
        #     assert isinstance(
        #         data, Iterable
        #     ), f"Unpack decorator only supports single iterable data, current node: {self.__name__}, current data: {data}"
        #     for d in data:
        #         yield d
        # else:
        #     yield data

    async def _put_data(self, data):
        """
        Puts data to the destination queue.
        """
        if self.discard_none_output and data is None:
            return
        for node in self.dst_nodes.values():
            if not self.criterias.get(node.__name__, None) or self.criterias[
                node.__name__
            ](data):
                if self.put_deepcopy_data:
                    await node.put(copy.deepcopy(data))
                else:
                    await node.put(data)

    def _error_decorator(self, func):

        # 1. 先用 Tenacity 包一层 retry，但排除 NodeStop
        retryer = retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential_jitter(max=10),
            retry=retry_if_exception_type(Exception) & retry_if_not_exception_type(NodeStop),
        )
        if inspect.iscoroutinefunction(func):
            @retryer
            @functools.wraps(func)
            async def input_wrapper(data):
                if self.no_input:
                    return await func()
                return await func(data)
        else:
            @retryer
            @functools.wraps(func)
            def input_wrapper(data):
                if self.no_input:
                    return func()
                return func(data)

        # 根据 func 是否异步，分别生成同步／异步的 error_wrapper
        if inspect.iscoroutinefunction(input_wrapper):
            @functools.wraps(input_wrapper)
            async def error_wrapper(data):
                try:
                    return await input_wrapper(data)
                except Exception as e:
                    logger.error(
                        f"{self.__name__} error: {e}, input_data: {data}"
                        f"Error stack:\n{traceback.format_exc()}"
                    )
                    if DEBUG_MOD:
                        raise
                    else:
                        return NodeProcessingError((data), self.__name__, e, traceback.format_exc())
        else:
            @functools.wraps(input_wrapper)
            def error_wrapper(data):
                try:
                    return input_wrapper(data)
                except Exception as e:
                    logger.error(
                        f"{self.__name__} error: {e}, input_data: {data}"
                        f"Error stack:\n{traceback.format_exc()}"
                    )
                    if DEBUG_MOD:
                        raise
                    else:
                        return NodeProcessingError((data), self.__name__, e, traceback.format_exc())

        return error_wrapper

    # 判断上游是否都运行结束了
    def _is_upstream_end(self):
        if self.src_nodes and all(
            node.is_start == False for node in self.src_nodes.values()
        ):
            return True
        else:
            return False
