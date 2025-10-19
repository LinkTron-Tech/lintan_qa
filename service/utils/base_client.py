from typing import Callable, Dict, Optional

class BaseAIClient:
    """AI 客户端基类，定义统一接口"""

    def completion(self, user_message: str, on_stream: Optional[Callable[[str, str], None]] = None) -> Dict[str, str]:
        """
        向模型提问
        :param user_message: 用户输入
        :param on_stream: 流式回调函数 (reasoning_content, content)
        :return: {'reasoning_content': str, 'content': str}
        """
        raise NotImplementedError

    def reset(self):
        """清空上下文"""
        raise NotImplementedError
