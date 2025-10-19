import logging
from typing import Dict, Any
from tenacity import retry, stop_after_attempt, after_log, retry_if_exception_type

from service.utils.llm_clients import create_ai_client
from service.prompts import prompt_manager
from service.utils.process_str import format_snippet_data,parse_llm_json_output
from base_method.module import Neuron, Module
import json

logger = logging.getLogger(__name__)


class QualityAssurance(Module):
    """
    评估 AnswerGeneration 模块生成的答案质量。
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        # self.quality_neuron = QualityNeuron(config["neuron"])
        logger.info(f"[QualityAssurance] Initialized with model: {config['neuron']['model']}")

    async def forward(self, pipeline_data: Dict[str, Any]) -> Dict:
        # request_id = pipeline_data.get("id")

        # # 从新的数据结构中获取答案
        # answer_output = pipeline_data.get("origin_answer", {})

        # # 检查上游模块的状态
        # # 如果上游生成失败、不相关或无知识，则直接跳过评估
        # if not answer_output:
        #     logger.info(f"ID: {request_id}, 没有输出'，跳过质量评估。")
        #     pipeline_data["quality"] = {
        #         "is_correct": False,
        #         "qa_status": "skipped",
        #         "feedback": "上游未成功生成答案，跳过质量评估。"
        #     }
        #     return pipeline_data

        # try:
        #     # 传递完整的 pipeline_data 给 neuron
        #     quality_result = await self.quality_neuron(pipeline_data)
        #     logger.info(f"ID: {request_id}, 评估结果: {quality_result}")

        #     is_correct = str(quality_result.get("qa_status", "false")).lower() == "true"
        #     pipeline_data["quality"] = {
        #         "is_correct": is_correct,
        #         **quality_result  # 将原始评估结果也放进去
        #     }

        # except Exception as e:
        #     logger.error(f"ID: {request_id}, 评估节点执行失败: {e}", exc_info=True)
        #     pipeline_data["quality"] = {
        #         "is_correct": False,  # 出错时默认为 False
        #         "qa_status": "error",
        #         "feedback": "评估出错"
        #     }
        #     pipeline_data["error"] = pipeline_data.get("error", "") + f"Quality assurance failed: {e}"



        return pipeline_data


class QualityNeuron(Neuron):
    """
    LLM 推理神经元，负责 prompt 构建、模型调用、JSON 解析，用于质量评估。
    """

    def __init__(self, llm_config: Dict[str, Any]):
        super().__init__()
        self.prompt_manager = prompt_manager
        self.req_pool = ChatClient(
            model=llm_config["model"],
        )

    @retry(
        stop=stop_after_attempt(1),
        after=after_log(logger, logging.WARNING),
        retry=retry_if_exception_type((ValueError, json.JSONDecodeError))
    )
    async def forward(self, pipeline_data: Dict[str, Any], lang='zh') -> Dict:
        request_id = pipeline_data.get("id")
        question = pipeline_data.get("question")
        knowledge_result = pipeline_data.get("knowledge", {})
        messages = pipeline_data.get("messages", [])  # 获取历史消息
        answer_to_evaluate = pipeline_data.get("origin_answer", {})

        if not question or not answer_to_evaluate:
            logger.error(
                f"QualityNeuron missing 'question' or valid 'answer_output' in input data for ID: {request_id}")
            raise ValueError("Missing 'question' or valid 'answer_output' in input data for QualityNeuron.")

        kn_snippets = knowledge_result.get("snippets", [])
        snippets_for_llm = format_snippet_data(kn_snippets)

        prompt, _ = self.prompt_manager.get_prompt("Quality_Assurance", lang=lang)
        retrieval_schema, _ = self.prompt_manager.get_prompt("Quality_Assurance_schema", lang=lang)

        prompt = prompt.format(
            model_answer=answer_to_evaluate, 
            snippets=snippets_for_llm
        )

        messages_for_llm = [
            {"role": "system", "content": prompt}
        ]

        

        if messages:
            recent_messages = messages[-6:]  # 截取最近的N条消息

            for msg in recent_messages:
                role_type_value = msg.get('role_type') # 获取 role_type 的值
                content = msg.get('content')

                if not content or role_type_value is None:
                    continue

                role = ""
                # 统一处理 role_type，无论是整数还是字符串
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
                        elif role_type_str in ['ASSISTANT', 'ASSISANT']: # 兼容拼写错误
                            role = 'assistant'

                if not role:
                    logger.warning(f"Skipping message with unknown or incompatible role_type: {role_type_value}")
                    continue
                
                messages_for_llm.append({"role": role, "content": content})

        messages_for_llm.append(
            {"role": "user", "content": question}
        )

        result_str = await self.req_pool.completion(messages=messages_for_llm, json_schema=retrieval_schema)

        quality_result = parse_llm_json_output(result_str)
        return quality_result

