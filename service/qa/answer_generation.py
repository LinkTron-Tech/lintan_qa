import asyncio
import logging
import random
import re
from typing import Dict, Any, Optional, List
import redis.asyncio as aioredis
from tenacity import retry, stop_after_attempt, after_log, retry_if_exception_type

from service.utils.llm_clients import create_ai_client
from service.prompts import prompt_manager
from service.utils.process_str import parse_llm_json_output,format_snippet_data,parse_llm_though_output,normalize_span_citations
from base_method.module import Neuron, Module
import json

logger = logging.getLogger(__name__)

PIPELINE_THOUGHT_STREAM_PREFIX = "qa-thought-stream:"

PIPELINE_STREAM_PREFIX = "qa-stream:"

class AnswerGeneration(Module):
    """
    """

    def __init__(self, config: Dict[str, Any], redis_client: Optional[aioredis.Redis] = None):
        super().__init__()
        self.answer_neuron = AnswerGenNeuron(config["neuron"], redis_client)
        logger.info(f"[AnswerGeneration] Initialized with model: {config['neuron']['model']}")


    async def forward(self, pipeline_data: Dict[str, Any]) -> Dict:
        """
        pipeline_data 预期包含 'id', 'question', 'messages', 'intent', 'knowledge'
        """
        request_id = pipeline_data.get("id")
        question = pipeline_data.get("question")
        messages = pipeline_data.get("messages", [])  # 确保传递 messages
        intent_result = pipeline_data.get("intent")  # 从上游获取意图
        knowledge_result = pipeline_data.get("knowledge")  # 从上游获取知识召回结果

        if not intent_result or not isinstance(intent_result, dict):
            logger.error(f"AnswerGeneration received invalid or missing 'intent' data for ID: {request_id}")
            pipeline_data["origin_answer"] = "无法生成答案：意图数据缺失或无效。"
            pipeline_data["error"] = pipeline_data.get("error",
                                                       "") + "Missing or invalid intent data for answer generation."
            return pipeline_data

        is_relevant = intent_result.get("is_relevant_topic", False)

        if is_relevant:
            if not knowledge_result or not knowledge_result.get("snippets"):
                logger.warning(f"ID: {request_id}, 意图相关但未找到知识切片。返回固定提示。")
                pipeline_data["origin_answer"] = "暂时知识库无相关文件，相关文件正在补充中。"
                return pipeline_data
            try:
                
                answer,though = await self.answer_neuron.forward(pipeline_data)
                logger.info(f"ID: {request_id}, 模型生成的答案是: {answer}")
                pipeline_data["origin_answer"] = answer
                pipeline_data["ch_though"] = though  # 存储翻译后的思考过程
            except Exception as e:
                logger.error(f"ID: {request_id}, 模型输出失败: {e}", exc_info=True)
                pipeline_data["origin_answer"] = "模型输出失败"
                pipeline_data["error"] = pipeline_data.get("error", "") + f"Answer generation failed: {e}"
        else:
            logger.info(f"ID: {request_id}, 意图不相关，跳过答案生成。")
            pipeline_data["origin_answer"] = "根据意图判断，您的提问不属于服务范围，无法提供相关答案。"

        return pipeline_data

class AnswerGenNeuron(Neuron):
    """
    负责 prompt 构建、模型调用，生成原始答案。
    """

    def __init__(self, llm_config: Dict[str, Any], redis_client: Optional[aioredis.Redis] = None):
        super().__init__()
        self.redis_client = redis_client
        self.prompt_manager = prompt_manager
        self.req_pool = create_ai_client(
            model=llm_config["model"]
        )


    @retry(
        stop=stop_after_attempt(2),
        after=after_log(logger, logging.WARNING),
        retry=retry_if_exception_type((ValueError, json.JSONDecodeError))
    )

    async def forward(self, pipeline_data: Dict[str, Any], lang='zh') -> (str, str):

        question = pipeline_data.get("question")
        knowledge_result = pipeline_data.get("knowledge")
        request_id = pipeline_data.get("id")

        rewritten_question = pipeline_data.get("intent").get("rewritten_question", question)

        kn_snippets = knowledge_result.get("snippets", []) if knowledge_result else []

        # 获取 prompt

        assemble_prompt = format_snippet_data(kn_snippets)
        logger.info(f"ID: {request_id}, 组装后的提示词片段: {assemble_prompt}")

        prompt, user_lang = self.prompt_manager.get_prompt("Origin_Answer_Generation", lang=lang)
        prompt = prompt.format(filtered_snippets=assemble_prompt)


        result =await self.req_pool.completion(user_message = rewritten_question,system_prompt=prompt,stream=True)

        ch_though,answer_content_raw = result["reasoning_content"], result["content"]

        answer_content_raw = normalize_span_citations(answer_content_raw)

        await self._stream_text_to_redis(request_id, ch_though)

        logger.info(f"ID: {request_id}, Redis 推送任务已作为后台任务启动。")

        await self._stream_new_correction(request_id,answer_content_raw)

        return answer_content_raw,ch_though
    

    async def _stream_new_correction(self, request_id, answer_content_raw: str) -> str:
        if not self.redis_client or not request_id or not text:
            return

        full_corrected_answer = answer_content_raw  
        stream_key = f"{PIPELINE_STREAM_PREFIX}{request_id}"
        buffer = answer_content_raw

        try:
            spans_to_send = ""
            # 提取所有完整的 <span> 块
            while '<span>' in buffer and '</span>' in buffer:
                match = re.search(r'(.*?)<span>(.*?)</span>(.*)', buffer, re.DOTALL)
                if match:
                    part_to_send = match.group(1) + f"<span>{match.group(2)}</span>"
                    spans_to_send += part_to_send
                    buffer = match.group(3)
                else:
                    break

            # 规范化并推送
            spans_to_send = normalize_span_citations(spans_to_send)
            if spans_to_send:
                logger.info(f"推送 span 内容: {spans_to_send}")
                await self.redis_client.rpush(stream_key, spans_to_send)
                await self.redis_client.expire(stream_key, 3600)

            # 如果 buffer 还有剩余内容，也推送
            if buffer:
                await self.redis_client.rpush(stream_key, buffer)
                await self.redis_client.expire(stream_key, 3600)

            return full_corrected_answer

        except Exception as e:
            logger.error(f"ID: {request_id}, 处理修正文案失败: {e}", exc_info=True)
            return answer_content_raw


    async def _stream_text_to_redis(self, request_id: str, text: str):
        if not self.redis_client or not request_id or not text:
            return

        stream_key = f"{PIPELINE_THOUGHT_STREAM_PREFIX}{request_id}"
        min_delay, max_delay = 0.2, 0.5

        # 定义需要完整保护的“原子区块”模式，这里只考虑思考过程中的 markdown 格式
        protection_pattern = re.compile(
            r'(\*\*(.*?)\*\*|\`\`\`.*?`\`\`|.*?\n)',
            re.DOTALL
        )

        # 定义普通文本的分割模式 (按标点和换行)
        punctuation_pattern = re.compile(r'([，；。？！,;?!;\n])')

        # --- 核心分割逻辑 ---
        final_chunks: List[str] = []

        # 第一次分割：分离出“保护区”和“普通文本区”
        parts = protection_pattern.split(text)

        for i, part in enumerate(parts):
            if not part:
                continue

            # 如果 part 是由 protection_pattern 分割出来的分隔符（即保护区本身）
            if i % 2 == 1:
                final_chunks.append(part)
            else:
                # 这是一个普通文本区，需要进行第二次分割（按标点）
                sub_parts = punctuation_pattern.split(part)

                temp_chunk = ""
                for j in range(len(sub_parts)):
                    temp_chunk += sub_parts[j]
                    if j % 2 == 1 or (j == len(sub_parts) - 1 and temp_chunk):
                        if temp_chunk.strip():
                            final_chunks.append(temp_chunk)
                        temp_chunk = ""

        # --- 流式推送 ---
        try:
            logger.info(f"ID: {request_id}, 开始模拟流式推送思考过程...")
            for chunk in final_chunks:
                logger.info(f"模拟持续推送: {chunk}")
                await self.redis_client.rpush(stream_key, f"{chunk}")
                await self.redis_client.expire(stream_key, 3600)
                delay = random.uniform(min_delay, max_delay)
                await asyncio.sleep(delay)
            logger.info(f"ID: {request_id}, 模拟流式推送完成。")
        except Exception as e:
            logger.error(f"ID: {request_id}, 模拟流式推送时发生错误: {e}", exc_info=True)

