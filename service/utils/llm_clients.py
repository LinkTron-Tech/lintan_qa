from typing import List, Dict, Optional, Callable, Union
from openai import OpenAI,AsyncOpenAI
from .base_client import BaseAIClient
from flowsettings import LLM_VENDOR_CONFIGS


class DeepSeekClient(BaseAIClient):
    """
    DeepSeek 通用客户端：
    - 支持任意模型调用（如 deepseek-chat / deepseek-reasoner）
    - 支持动态 system_prompt / response_format / stream
    - 可选上下文保留
    """

    def __init__(self,model: str):
        cfg = LLM_VENDOR_CONFIGS["deepseek"]
        self.client = AsyncOpenAI(api_key=cfg["api_key"], base_url=cfg["base_url"])
        self.messages: List[Dict[str, str]] = []
        self.model = model

    def reset(self):
        """清空上下文"""
        self.messages = []

    async def completion(
        self,
        user_message: str,
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        stream: bool = False,
        response_format: Optional[Union[bool, Dict]] = None,
        on_stream: Optional[Callable[[str, str], None]] = None,
        keep_context: bool = False
    ) -> Dict[str, str]:
        """
        调用 DeepSeek 模型生成结果。

        :param user_message: 用户输入
        :param system_prompt: 可选系统提示词
        :param model: 模型名称（默认取配置里的 default_model）
        :param stream: 是否启用流式输出
        :param response_format: 是否使用 JSON 输出格式
        :param on_stream: 流式回调函数
        :param keep_context: 是否保留上下文（多轮对话）
        """
        cfg = LLM_VENDOR_CONFIGS["deepseek"]
        model = model or self.model

        # 动态构造消息
        messages = list(self.messages) if keep_context else []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_message})

        # response_format 参数处理
        kwargs = {}
        if response_format:
            kwargs["response_format"] = (
                {"type": "json_object"} if response_format is True else response_format
            )

        reasoning_content, content = "", ""

        # === 执行调用 ===
        if stream:
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                stream=True,
                **kwargs
            )
            async for chunk in response:
                delta = chunk.choices[0].delta
                if hasattr(delta, "reasoning_content") and delta.reasoning_content:
                    reasoning_content += delta.reasoning_content
                if hasattr(delta, "content") and delta.content:
                    content += delta.content
                if on_stream:
                    on_stream(reasoning_content, content)
        else:
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                **kwargs
            )
            if getattr(response.choices[0].message, "reasoning_content", None):
                reasoning_content = response.choices[0].message.reasoning_content
            content = response.choices[0].message.content

        # === 记录上下文 ===
        if keep_context:
            self.messages.extend([
                {"role": "user", "content": user_message},
                {"role": "assistant", "content": content}
            ])

        return {"reasoning_content": reasoning_content, "content": content}



# ------------------ 工厂函数 ------------------
def create_ai_client(provider = "deepseek", model: Optional[str] = None, stream: bool = False, response_format: Optional[dict] = None) -> BaseAIClient:
    provider = provider.lower()
    if provider not in LLM_VENDOR_CONFIGS:
        raise ValueError(f"Unsupported provider: {provider}")

    cfg = LLM_VENDOR_CONFIGS[provider]

    if provider == "deepseek":
        return DeepSeekClient(
            model=model or cfg["default_model"]
        )
    else:
        raise ValueError(f"Provider {provider} exists in config but no client implementation.")





if __name__ == "__main__":
    import os
    from time import sleep

    # ------------------ 配置 ------------------
    DEEPSEEK_API_KEY = "sk-ab0224e74e80457e928badd27ff99c22"
    if not DEEPSEEK_API_KEY:
        raise ValueError("请先设置环境变量 DEEPSEEK_API_KEY")

    # ------------------ 测试非流式 ------------------
    print("=== 非流式测试 ===")
    client = create_ai_client(
        "deepseek"
    )

    resp = client.completion(
        user_message="请生成一个包含title和summary的JSON对象。",
    response_format=True,)
    print("Reasoning:", resp["reasoning_content"])
    print("Content:", resp["content"])
    print("-" * 50)

    # ------------------ 测试流式 ------------------
    print("=== 流式测试 ===")

    def stream_callback(reasoning, content):
        print("\rContent so far:", content, end="")

    client_stream = create_ai_client(
        "deepseek"
    )

    resp_stream = client_stream.completion(
        "Please summarize the story of the tortoise and the hare in JSON format.",
        stream = True,
        on_stream=stream_callback
    )
    print("\nFinal Reasoning:", resp_stream["reasoning_content"])
    print("Final Content:", resp_stream["content"])
    print("-" * 50)

    # ------------------ 多轮对话 ------------------
    print("=== 多轮对话测试 ===")
    client.reset()
    print("User: What's the capital of France?")
    resp1 = client.completion("What's the capital of France?")
    print("Assistant:", resp1["content"])

    print("User: And the population?")
    resp2 = client.completion("And the population?")
    print("Assistant:", resp2["content"])
