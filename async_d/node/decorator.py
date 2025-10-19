import functools
import logging
import inspect

from ..label import LabelData

logger = logging.getLogger(__name__)


def skip_data_decorator(func):
    """
    如果 func 是 coroutine，就用 async wrapper 并 await，
    否则用普通同步 wrapper。
    """
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def skip_wrapper(data):
            if isinstance(data, Exception):
                return data
            # 异步调用
            return await func(data)
        return skip_wrapper
    else:
        @functools.wraps(func)
        def skip_wrapper(data):
            if isinstance(data, Exception):
                return data
            return func(data)
        return skip_wrapper


def label_proc_decorator(func):
    """
    对带 LabelData 的数据做打标处理，兼容 sync/async func。
    """
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def labeled_wrapper(label_data):
            assert isinstance(label_data, LabelData)
            label = label_data.label
            # 异步调用
            result = await func(label_data.data)
            return LabelData(result, label)
        return labeled_wrapper
    else:
        @functools.wraps(func)
        def labeled_wrapper(label_data):
            assert isinstance(label_data, LabelData)
            label = label_data.label
            result = func(label_data.data)
            return LabelData(result, label)
        return labeled_wrapper
