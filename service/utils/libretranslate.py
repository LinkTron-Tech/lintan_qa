# coding=utf-8
import os
import requests
import time
import hashlib
import json
import logging
import asyncio
from typing import Dict, Any

logger = logging.getLogger(__name__)

# --- 小牛翻译 API 配置 ---
apiUrl = "https://api.niutrans.com"
transUrl = apiUrl + "/v2/text/translate"  # 上传并翻译接口
APP_ID = "vgf1755151200204"
API_KEY = "a4a0b282af7671345183fadaaa2ec8d5"


def _generate_auth_str(params: Dict[str, Any]) -> str:
    """
    生成小牛翻译API所需的鉴权字符串。
    """
    sorted_params = sorted(list(params.items()) + [('apikey', API_KEY)], key=lambda x: x[0])
    param_str = '&'.join([f'{key}={value}' for key, value in sorted_params])
    md5 = hashlib.md5()
    md5.update(param_str.encode('utf-8'))
    auth_str = md5.hexdigest()
    return auth_str


# 异步函数封装，用于翻译文本
async def translate_text(src_text: str, from_language: str = "en", to_language: str = "zh") -> Dict:
    """
    使用小牛翻译 API 异步翻译文本。

    Args:
        src_text: 要翻译的源文本。
        from_language: 源语言代码 (例如: "en")。
        to_language: 目标语言代码 (例如: "zh")。

    Returns:
        包含翻译结果的字典。
    """
    # 鉴于 requests 库是同步阻塞的，我们需要在线程池中运行它以避免阻塞事件循环
    loop = asyncio.get_event_loop()

    data = {
        'from': from_language,
        'to': to_language,
        'appId': APP_ID,
        'timestamp': int(time.time()),
        'srcText': src_text
    }
    print("src_text:",src_text)
    auth_str = _generate_auth_str(data)
    data['authStr'] = auth_str

    try:
        # response = await loop.run_in_executor(None, lambda: requests.post(transUrl, data=data))
        response =  requests.post(transUrl, data=data)
        logger.info(f"小牛翻译 API 请求成功: {response}")
        response_json = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"小牛翻译 API 请求失败: {e}")
        return src_text  # 返回原始文本，避免中断

    if "error_code" in response_json and response_json["error_code"] != "0":
        logger.error(f"小牛翻译 API 调用失败: {response_json}")
        return src_text  # 返回原始文本，避免中断

        # 如果成功，只返回翻译后的文本

    return response_json.get("tgtText", "")


# --- 使用示例 ---
async def main():
    global APP_ID, API_KEY
    APP_ID = "vgf1755151200204"
    API_KEY = "a4a0b282af7671345183fadaaa2ec8d5"

    src_text_to_translate = """
   
   '**Determining Core Issues**', "I'm now focusing on the core question: How to handle soil and water loss from construction projects? I need to classify the relevance and geographical scope. The context helps, but I am also analyzing for the user's specific needs. I'll create retrieval queries that are location-aware and leverage keywords from the main question.", '**Defining Key Aspects**', 'I\'m now focused on breaking down the user\'s prompt: "生产建设项目或活动造成水土流失不治理的，如何处理？" and evaluating the associated chat history. The history presents a bit of ambiguity regarding "刚刚那个问题". I\'ve clarified that the initial question focus is on identifying relevance to soil and water conservation, the geographic scope, and formulating effective retrieval queries.', '**Refining Question Analysis**', 'I\'ve re-evaluated the chat history, pinpointing a crucial distinction. "刚刚那个问题" refers to a previous question. The core prompt is "生产建设项目或活动造成水土流失不治理的，如何处理？" My assessment reveals direct relevance to soil and water conservation, confirming `is_relevant_scope defaults to "全国" due to a lack of specific region information. I am now formulating precise retrieval queries incorporating legal terms, current regulations, and the central concerns for refined and targeted information retrieval.', "**Evaluating the User's Inquiry**", 'I\'m in the final stages of analysis, now focusing on crafting retrieval queries. Based on the prompt about untreated soil and water loss and the chat history\'s context of "latest regulations," I\'ve formulated three targeted queries. I\'m prioritizing search terms that directly address legal repercussions, supervision methods, and the core issue, ensuring the most relevant and updated results.', '**Query Optimization Complete**', "I've completed generating three optimized retrieval queries. They are designed to address the core issue of untreated soil and water loss from construction projects. The queries focus on legal responsibility, current regulations, and supervision methods to ensure the user receives the most relevant and up-to-date information.", '**Query Generation Complete**', "I've now finalized my three retrieval queries. They are tailored to address the central issue of unmanaged soil and water loss in construction projects. They aim to pinpoint information on legal liabilities, supervisory methods, and punitive measures to deliver the most relevant and up-to-date results to the user.'
   
   """
    try:
        translated_result = await translate_text(src_text_to_translate)
        if "error" in translated_result:
            print(f"翻译失败: {translated_result['error']}")
        else:
            print("翻译结果：")
            print(translated_result)
    except Exception as e:
        print(f"在调用翻译函数时发生异常: {e}")


if __name__ == "__main__":
    asyncio.run(main())