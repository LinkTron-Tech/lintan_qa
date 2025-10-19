import asyncio
from openai import InternalServerError, RateLimitError, APIError, AsyncOpenAI
from setting import settings as flowsettings
import httpx
from tenacity import (
    retry,
    RetryError,
    stop_after_attempt,
    wait_random_exponential,
    before_sleep_log,
    retry_if_exception_type
)
import logging
from typing import AsyncGenerator, List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class ChatClient:
    def __init__(self, model="gemini-2.5-flash", api_key=flowsettings.OPENAI_API_KEY,
                 base_url=flowsettings.OPENAI_API_BASE, proxy_url=flowsettings.PROXY_URL):
        self._http_client = httpx.AsyncClient(
            proxy=proxy_url
        )
        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url,
            http_client=self._http_client
        )
        self.model = model

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        if not self._http_client.is_closed:
            await self._http_client.aclose()

    @retry(
        wait=wait_random_exponential(multiplier=2, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((RateLimitError, InternalServerError, APIError, ValueError))  # 如果不是这几个错就不retry了
    )
    async def completion(self, messages: List[Dict[str, str]], json_schema=None, include_thoughts=None ):
        kwargs = {
            "model": self.model,
            "messages": messages,
        }

        if json_schema:  # 非空才加 response_format
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": json_schema.get("name", ""),
                    "schema": json_schema.get("content", {})
                }
            }

        if include_thoughts:
            kwargs["extra_body"]={
                    'extra_body': {
                        "google": {
                            "thinking_config": {
                                "thinking_budget": 800,
                                "include_thoughts": True
                            }
                        }
                    }
                }

        try:
            response = await self.client.chat.completions.create(**kwargs)
            # logger.info(f"response{response}")
            # 新增检查：确保响应包含有效的 choices 数据
            if not response.choices or len(response.choices) == 0:
                error_msg = "OpenAI API returned empty choices in response"
                logger.debug(error_msg)
                raise ValueError(error_msg)
            answer = response.choices[0].message.content
            # logger.info(f"answer:{answer}")

        except RateLimitError as e:
            logger.warning(f"Rate limit exceeded in OpenAIRequest.completion: {e}")
            raise
        except InternalServerError as e:
            logger.warning(f"Internal server error in OpenAIRequest.completion: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in OpenAIRequest.completion: {e}. messages: \n{messages}")
            raise

        return answer

    @retry(
        wait=wait_random_exponential(multiplier=2, max=60),
        stop=stop_after_attempt(3),  
        retry=retry_if_exception_type((RateLimitError, InternalServerError, APIError, ValueError))
    )
    async def stream_completion(self, messages: List[Dict[str, str]], include_thoughts=None) -> AsyncGenerator[str, None]:
        """
        以流式方式调用 LLM 并逐块返回内容。
        """
        kwargs = {
            "model": self.model,
            "messages": messages,
            "stream": True,
        }

        # 如果需要包含思考过程，添加 extra_body
        if include_thoughts:
            kwargs.update({
                "extra_body": {
                    'extra_body': {
                        "google": {
                            "thinking_config": {
                                "thinking_budget": 800,
                                "include_thoughts": True
                            }
                        }
                    }
                }
            }
            )
        try:
            stream = await self.client.chat.completions.create(**kwargs)
            async for chunk in stream:
                content = chunk.choices[0].delta.content or ""
                yield content
        except Exception as e:
            logger.error(f"Unexpected error in stream_completion: {e}. messages: \n{messages}")
            raise


# --- 主测试函数 ---
async def main():
    client = ChatClient()

    print("--- 场景一：不包含思考过程的流式输出 ---")
    try:
        # async for chunk in client.stream_completion("解释一下人工智能的工作原理"):
        #     print(chunk, end='', flush=True)
        result = await client.completion({"role":"user", "content":"只是测试一下"}, include_thoughts=True)
        print("result",result)
        print("\n\n--- 场景一测试完成。---")
    except Exception as e:
        print(f"\n场景一测试失败：{e}")

    # print("\n" + "=" * 50 + "\n")
    #
    # print("--- 场景二：包含思考过程的流式输出 ---")
    # print("提问：监督式学习和非监督式学习有什么区别？")
    # try:
    #     async for chunk in client.stream_completion("监督式学习和非监督式学习有什么区别？", include_thoughts=True):
    #         print(chunk, end='', flush=True)
    #     print("\n\n--- 场景二测试完成。---")
    # except Exception as e:
    #     print(f"\n场景二测试失败：{e}")


# 运行主函数
if __name__ == "__main__":
    asyncio.run(main())
    # main()
