import logging
from typing import Dict, Any,List
from tenacity import retry, stop_after_attempt, after_log, retry_if_exception_type
import asyncio
from datetime import datetime, timedelta
from service.utils.llm_clients import create_ai_client
from service.prompts import prompt_manager
from service.utils.process_str import parse_llm_json_output,parse_llm_though_output
from service.utils.libretranslate import translate_text
from base_method.module import Neuron, Module
import json

logger = logging.getLogger(__name__)


class IntentUnderstander(Module):
    """
    意图理解模块
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.intent_neuron = IntentNeuron(config["neuron"])
        logger.info(f"[IntentUnderstander] Initialized with model: {config['neuron']['model']}")

    async def forward(self, input_data: Dict[str, Any]) -> Dict:
        """
        使用大模型理解用户问题意图。
        """

        # 提取关键数据
        request_id = input_data.get("id")
        question = input_data.get("question")
        messages = input_data.get("messages", [])  # 获取消息列表，如果不存在则为空列表

        if not question:
            logger.error(f"IntentUnderstander received input_data without 'question'. ID: {request_id}")
            # 返回一个错误结构，确保包含 ID，以便 QAPipelineServiceServicer 可以处理
            return {
                "id": request_id,
                "error": "Missing 'question' in input data",
                "intent": {
                    "is_relevant_topic": False,  "rewritten_question": "",
                },
                "question": question,
                "messages": messages
            }

        try:
            intent_result = await self.intent_neuron(question=question, messages=messages)
            logger.info(f"ID: {request_id}, 用户问题意图: {intent_result}")

            # 返回一个包含所有原始输入数据和新生成意图的字典
            return {
                "id": request_id,
                "question": question,
                "messages": messages,  
                "intent": intent_result
            }
        except Exception as e:
            logger.warning(f"ID: {request_id}, 用户问题意图解析失败: {e}", exc_info=True)
            return {
                "id": request_id,
                "intent": {
                    "is_relevant_topic": False, "rewritten_question": "",
                },
                "question": question,
                "messages": messages, 
                "error": str(e)
            }


class IntentNeuron(Neuron):
    """
    负责 prompt 构建、模型调用、JSON 解析。
    """

    def __init__(self, llm_config: Dict[str, Any]):
        super().__init__()
        self.prompt_manager = prompt_manager
        self.req_pool = create_ai_client(
            
            model=llm_config["model"]
        )

    @retry(
        stop=stop_after_attempt(2),
        after=after_log(logger, logging.WARNING),
        retry=retry_if_exception_type((ValueError, json.JSONDecodeError))
    )
    async def forward(self, question: str,messages: List[Dict[str, str]], lang='zh') -> Dict:
        logger.info(f"用户的messages: {messages}")

        # 1. 统一准备基础消息历史 (user/assistant turns)
        rewrite_messages = []
        if messages:
            recent_messages = messages[-6:]
            for msg in recent_messages:
                role_type_value = msg.get('role_type') # 获取 role_type 的值
                content = msg.get('content')
                if not content or role_type_value is None:
                    continue

                role = ""
                try:
                    # 尝试将值转换为整数，以处理 gRPC 枚举的常见情况
                    role_type_int = int(role_type_value)
                    if role_type_int == 1: # USER = 1
                        role = 'user'
                    elif role_type_int == 2: # ASSISTANT = 2
                        role = 'assistant'
                    
                except (ValueError, TypeError):
                    if isinstance(role_type_value, str):
                        role_type_str = role_type_value.upper()
                        if role_type_str == 'USER':
                            role = 'user'
                        elif role_type_str == 'ASSISTANT': 
                            role = 'assistant'

                if role: 
                    rewrite_messages.append({"role": role, "content": content})

        rewrite_messages.append(
            {"role": "user", "content": question}
        )

        # --- 步骤 1 : 执行对话改写 & 主题校验 ---
        logger.info("执行步骤1：重写和验证...")

        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

        rewrite_prompt, _ =  self.prompt_manager.get_prompt("REWRITE_AND_VALIDATE", lang=lang)
        rewrite_prompt = rewrite_prompt.format(
            today = today,
            yesterday = yesterday,
            tomorrow = tomorrow,
            conversation=rewrite_messages
        )


        rewrite_result = await self.req_pool.completion(user_message=rewrite_prompt,response_format=True)
        logger.info(f"意图识别结果：{rewrite_result}")

        rewrite_result = parse_llm_json_output(rewrite_result.get("content",""))

        # --- 步骤 2 : 如果不相关，则提前退出 ---
        if not rewrite_result or not rewrite_result.get("is_relevant"):
            logger.warning("主题不相关或重写失败。停止处理。")
            return {"is_relevant_topic": False, "retrieval_queries": []}

        rewritten_question = rewrite_result.get("rewritten_question") or question
        logger.info(f"question: {question}, Rewritten question for parallel tasks: '{rewritten_question}'")

        return {
            "is_relevant_topic": True,
            "rewritten_question": rewritten_question
        }
        
        

    

import asyncio
import logging
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    async def main():
        # 模拟配置
        config = {
            "neuron": {
                "model": "gemini-2.5-flash",
                "infer_type": "OpenAI"
            }
        }
        intent_understander = IntentUnderstander(config)
        question = {
        "id": "unique_chat_session_123",
        "question": "湖北华中师范大学内的项目水土补偿费怎么计算",
        "messages": [
        #   {"role": "user", "USER": "你好，我想问个水土问题。", "content_type": "text"},
        #   {"role": "assistant", "ASSISANT": "请问您有什么问题？", "content_type": "text"},
        ]
      }
        result = await intent_understander.forward(question)
        logger.info("解析结果：", result)

    asyncio.run(main())