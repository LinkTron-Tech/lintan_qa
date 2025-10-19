import asyncio
import logging
import random
import re
from typing import Dict, Any, Optional, List
import redis.asyncio as aioredis
from tenacity import retry, stop_after_attempt, after_log, retry_if_exception_type

from service.utils.llm_clients import create_ai_client
from service.prompts import prompt_manager
from base_method.module import Neuron, Module
from service.utils.process_str import parse_llm_json_output, format_snippet_data,normalize_span_citations
import json

logger = logging.getLogger(__name__)

# 定义流式输出的 Redis Key 前缀
PIPELINE_STREAM_PREFIX = "qa-stream:"


class Answer_Correct(Module):
    """
    答案矫正与最终流式输出模块。
    本模块作为流程节点，负责调用Neuron来执行具体的流式输出任务。
    """

    def __init__(self, config: Dict[str, Any], redis_client: Optional[aioredis.Redis] = None):
        super().__init__()
        # self.correct_neuron = CorrectNeuron(config["neuron"], redis_client)
        logger.info(f"[Answer_Correct] Initialized with model: {config['neuron']['model']}")

    async def forward(self, pipeline_data: Dict[str, Any]) -> Dict:
        # """
        # 调用 Neuron 的 forward 方法，并将最终答案打包。
        # """
        request_id = pipeline_data.get("id")
        logger.info(f"--- [Answer_Correct] Entering for ID: {request_id} ---")
        pipeline_data["correct_output"] = {
                "final_answer": pipeline_data.get("origin_answer", ""),
                "status": "streamed"
            }

        # try:
        #     # 直接调用 Neuron 的 forward 方法，它会处理所有决策和流式逻辑
        #     final_answer = await self.correct_neuron.forward(pipeline_data)

        #     # 将最终结果存入 pipeline_data，使用您期望的字段
        #     # 注意：旧的 "correct" 字段结构被新的、更简单的 "final_answer" 替代
              # pipeline_data["final_answer"] = final_answer

        #     # 也可以保留一个更详细的输出结构
        #     pipeline_data["correct_output"] = {
        #         "final_answer": final_answer,
        #         "status": "streamed"
        #     }

        # except Exception as e:
        #     logger.error(f"ID: {request_id}, 在最终输出阶段发生错误: {e}", exc_info=True)
        #     # 确保错误路径下的字段也与原始代码对齐
        #     pipeline_data["final_answer"] = pipeline_data.get("origin_answer", "处理出错")
        #     pipeline_data["correct_output"] = {
        #         "final_answer": pipeline_data.get("origin_answer", "处理出错"),
        #         "status": "error",
        #         "error_message": str(e)
        #     }

        # logger.info(
        #     f"--- [Answer_Correct] Exiting for ID: {request_id}, Final Answer: {pipeline_data.get('final_answer')}...")
        return pipeline_data


class CorrectNeuron(Neuron):
    """
    LLM 推理神经元，负责所有最终的决策和流式输出任务。
    """

    def __init__(self, llm_config: Dict[str, Any], redis_client: Optional[aioredis.Redis] = None):
        super().__init__()
        self.redis_client = redis_client
        self.prompt_manager = prompt_manager
        self.req_pool = ChatClient(
            model=llm_config["model"],
        )

    async def forward(self, pipeline_data: Dict[str, Any], lang='zh') -> str:

        request_id = pipeline_data.get("id")

        # 从上游的确切字段 "quality" 和 "origin_answer" 读取数据
        quality_data = pipeline_data.get("quality", {})
        original_answer = pipeline_data.get("origin_answer", "")

        # 解析上游 "quality" 字典中的 "qa_status" 字段
        qa_status_val = quality_data.get("qa_status")
        is_correct = False
        if isinstance(qa_status_val, str):
            is_correct = qa_status_val.lower() == "true"
        elif isinstance(qa_status_val, bool):
            is_correct = qa_status_val

        # 如果评估被跳过，也视为答案是“好”的
        if qa_status_val == "skipped":
            is_correct = True

        final_answer_text = original_answer

        if is_correct:
            # --- 情况 A: 答案质量好 (或跳过评估)，模拟流式输出原始答案 ---
            logger.info(f"ID: {request_id}, 答案质量达标或无需评估，模拟流式输出原始答案。")
            await self.stream_existing_text(request_id, original_answer)
            final_answer_text = original_answer
        else:
            # --- 情况 B: 答案质量差，真实流式生成修正答案 ---
            if original_answer:
                logger.info(f"ID: {request_id}, 答案质量不佳，开始流式生成修正答案。")
                final_answer_text = await self.stream_new_correction(pipeline_data, lang=lang)
            else:
                # 兜底情况：质量差但无原始答案
                logger.warning(f"ID: {request_id}, 答案质量不佳但无原始答案可修正，输出固定提示。")
                fallback_answer = "抱歉，我们暂时无法提供准确的答案。"
                await self.stream_existing_text(request_id, fallback_answer)
                final_answer_text = fallback_answer

        return final_answer_text

    async def stream_existing_text(self, request_id: str, text: str):
        """
        将已有文本以模拟LLM流的方式推送到Redis。
        采用多阶段分割策略，确保公式、HTML标签等特殊区块的完整性，
        同时对普通文本按标urenta符号进行自然的流式分割。
        """
        if not self.redis_client or not request_id or not text:
            return

        stream_key = f"{PIPELINE_STREAM_PREFIX}{request_id}"
        min_delay, max_delay = 0.2, 0.4

        # 1. 定义需要完整保护的“原子区块”模式
        # 包含：$$...$$ (块公式), $...$ (行内公式), <span>...</span>, ```...``` (代码块)
        # 注意：正则表达式中的 '$' 需要转义为 '\\$'
        # 's' flag (re.DOTALL) 使得 '.' 可以匹配换行符，对于多行公式和代码块至关重要
        protection_pattern = re.compile(
            r'(\$\$.*?\$\$|\$.*?\$|<span>.*?</span>|```.*?```)',
            re.DOTALL
        )

        # 2. 定义普通文本的分割模式 (按标点和换行)
        punctuation_pattern = re.compile(r'([，；。？！,;?!;\n])')

        # --- 核心分割逻辑 ---
        final_chunks: List[str] = []

        # 第一次分割：分离出“保护区”和“普通文本区”
        parts = protection_pattern.split(text)

        for i, part in enumerate(parts):
            if not part:
                continue

            # 如果 part 是由 protection_pattern 分割出来的分隔符（即保护区本身）
            # `split` 的工作方式是 [text, separator, text, separator, ...]
            # 所以索引为 1, 3, 5, ... 的是保护区
            if i % 2 == 1:
                # 这是一个受保护的区块，直接作为一个整体添加
                final_chunks.append(part)
            else:
                # 这是一个普通文本区，需要进行第二次分割（按标点）
                sub_parts = punctuation_pattern.split(part)

                # 重新组合文本和其后的标点
                temp_chunk = ""
                for j in range(len(sub_parts)):
                    temp_chunk += sub_parts[j]
                    if j % 2 == 1 or (j == len(sub_parts) - 1 and temp_chunk):
                        if temp_chunk:  # 避免添加纯空白块
                            final_chunks.append(temp_chunk)
                        temp_chunk = ""

        # --- 流式推送 ---
        try:
            logger.info(f"ID: {request_id}, 开始最终版模拟流式推送...")
            for chunk in final_chunks:
                logger.info(f"模拟持续推送: {chunk}")
                await self.redis_client.rpush(stream_key, chunk)
                await self.redis_client.expire(stream_key, 3600)
                delay = random.uniform(min_delay, max_delay)
                await asyncio.sleep(delay)
            logger.info(f"ID: {request_id}, 最终版模拟流式推送完成。")
        except Exception as e:
            logger.error(f"ID: {request_id}, 模拟流式推送时发生错误: {e}", exc_info=True)

    async def stream_new_correction(self, pipeline_data: Dict[str, Any], lang='zh') -> str:
        """
        调用 LLM 流式生成修正答案，实时推送到 Redis，并返回完整的修正后字符串。
        """
        request_id = pipeline_data.get("id")

        # 从上游的确切字段获取数据
        question = pipeline_data.get("question")
        original_answer = pipeline_data.get("origin_answer")
        knowledge_result = pipeline_data.get("knowledge", {})
        
        quality_data = pipeline_data.get("quality", {})  # 注意这里是 "quality"

        if not all([question, original_answer, knowledge_result, quality_data]):
            raise ValueError("CorrectNeuron 进行流式修正时缺少必要数据。")

        kn_snippets = knowledge_result.get("snippets", [])
        logger.info(f"知识库召回结果: {kn_snippets}")
        snippets_for_llm = format_snippet_data(kn_snippets)
        feedback_for_llm = json.dumps(quality_data, ensure_ascii=False)

        prompt, _ = self.prompt_manager.get_prompt("Answer_Correct", lang=lang)
        prompt = prompt.format(
            origin_user_input=question,
            model_answer=original_answer,
            snippets=snippets_for_llm,
            feedback=feedback_for_llm
        )

        messages_for_llm =  {"role": "user", "content": prompt}

        full_corrected_answer = ""
        stream_key = f"{PIPELINE_STREAM_PREFIX}{request_id}"
        buffer = ""
        try:
            async for chunk in self.req_pool.stream_completion(messages=messages_for_llm):
                full_corrected_answer += chunk
                buffer += chunk

                # 尝试从 buffer 中提取所有完整的 span 标签
                spans_to_send = ""
                while '<span>' in buffer and '</span>' in buffer:
                    # 找到第一个完整的 span
                    match = re.search(r'(.*?)<span>(.*?)</span>(.*)', buffer, re.DOTALL)
                    if match:
                        # 将 span 之前的部分和这个完整的 span 都发送出去
                        part_to_send = match.group(1) + f"<span>{match.group(2)}</span>"
                        spans_to_send += part_to_send

                        # 更新 buffer 为剩余部分
                        buffer = match.group(3)
                    else:
                        # 找不到完整的 span 了，退出循环
                        break

                spans_to_send = normalize_span_citations(spans_to_send)
                # 将本次提取到的所有完整部分推送到 Redis
                if spans_to_send:
                    logger.info(f"真实流式持续推送: {spans_to_send}")
                    await self.redis_client.rpush(stream_key, spans_to_send)
                    await self.redis_client.expire(stream_key, 3600)

                # 循环结束后，如果 buffer 中还有剩余内容，全部推送出去
            if buffer and self.redis_client:
                await self.redis_client.rpush(stream_key, buffer)
                await self.redis_client.expire(stream_key, 3600)

            return full_corrected_answer

        except Exception as e:
            logger.error(f"ID: {request_id}, 流式修正 LLM 调用失败: {e}", exc_info=True)
            return original_answer