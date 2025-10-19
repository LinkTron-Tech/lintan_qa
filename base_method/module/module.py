import asyncio
import inspect
import functools
from abc import ABC, abstractmethod
from typing import Dict, Optional

from ..data import Dataset
import logging
logger = logging.getLogger(__name__)


def parallel_semaphore_decorator(func):
    """
    异步信号量装饰器：串行控制 forward 的并发数。
    如果 func 是 async 函数，就用 async wrapper 并 await，
    如果 func 是 sync 函数，也用 async wrapper 便于统一调用。
    """
    if asyncio.iscoroutinefunction(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            async with self._parallel_semaphore:
                return await func(self, *args, **kwargs)
        return wrapper
    else:
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            async with self._parallel_semaphore:
                return func(self, *args, **kwargs)
        return wrapper


class Module(ABC):
    _parallel_count = 20

    def __init__(self, *args, **kwargs):
        self.__name__ = type(self).__name__
        self._modules: Dict[str, Module] = {}
        self._parallel_semaphore = asyncio.Semaphore(Module._parallel_count)

    async def __call__(self, *args, **kwargs):
        """
        支持两种调用方式：
        1) 单个 Dataset：并行调用 forward，返回结果列表，异常作为元素返回；
        2) 其它参数：直接 await forward(...)，异常向上抛出。
        """
            
        try:
            if len(args) == 1 and isinstance(args[0], Dataset):
                tasks = []
                batch_data = args[0]
                for data in batch_data:
                    # 不管 forward 本身是 sync 还是 async，都先拿到 coroutine
                    maybe_coro = self.forward(*data, **kwargs)
                    # 如果不是 coroutine，就用一个 0s sleep 包一层
                    if inspect.isawaitable(maybe_coro):
                        tasks.append(asyncio.create_task(maybe_coro))
                    else:
                        tasks.append(asyncio.create_task(asyncio.sleep(0, result=maybe_coro)))
                # 并行等待，return_exceptions=True 保证所有任务都收集结果
                return await asyncio.gather(*tasks, return_exceptions=True)
            else:
                result = self.forward(*args, **kwargs)
                if inspect.isawaitable(result):
                    return await result
                return result
        except Exception as e:
            # 打印完整的异常堆栈，包含模块名、方法名及出错行数
            logger.error(
                f"Error in module {self.__name__}, args={args}, kwargs={kwargs}: {e}",
                exc_info=True
            )
            raise

    @abstractmethod
    @parallel_semaphore_decorator
    async def forward(self, *args, **kwargs):
        """
        子类必须实现的业务处理函数，可以是异步或同步方法，
        但都以 async 接口暴露，并由 parallel_semaphore_decorator 控制并发。
        """
        pass

    def add_module(self, name: str, module: Optional["Module"]) -> None:
        if not isinstance(module, Module) and module is not None:
            raise TypeError(f"{type(module)} is not a Module subclass")
        elif not isinstance(name, str):
            raise TypeError(
                f"module name should be a string. Got {type(name)}"
            )
        elif hasattr(self, name) and name not in self._modules:
            raise KeyError(f"attribute '{name}' already exists")
        elif "." in name:
            raise KeyError(f'module name can\'t contain ".", got: {name}')
        elif name == "":
            raise KeyError('module name can\'t be empty string ""')
        self._modules[name] = module

    def _get_name(self):
        return self.__class__.__name__
