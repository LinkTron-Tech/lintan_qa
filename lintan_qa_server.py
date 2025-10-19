import grpc
from concurrent import futures
import asyncio
import json
import logging
import time
import redis.asyncio as aioredis

import qa_service_pb2
import qa_service_pb2_grpc

from grpc_reflection.v1alpha import reflection

from async_d import Node, Sequential, Monitor, PipelineAnalyser
from typing import Dict, Any, List
from asyncio import Semaphore
from setting import settings as flowsettings

from service.qa.intent_understander import IntentUnderstander
from service.qa.search_knowledge import SearchKnowledge
from service.qa.answer_generation import AnswerGeneration
from service.qa.quality_assurance import QualityAssurance
from service.qa.answer_correct import Answer_Correct
from base_method.module import Module
from logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_redis_client: aioredis.Redis = None

# --- Redis Key 前缀常量 ---
PIPELINE_STATUS_PREFIX = "qa:"
PIPELINE_STREAM_PREFIX = "qa-stream:"
PIPELINE_THOUGHT_STREAM_PREFIX = "qa-thought-stream:"


# --- 用于包装模块 forward 方法以实现 Redis 状态更新的辅助函数 ---
def create_persisted_module_wrapper(module: Module, stage_name: str) -> Module:
    """
    包装 Module 的 forward 方法，将其当前所处阶段的名称持久化到 Redis。
    Redis 键格式为 'qa:{request_id}'，值为当前阶段的名称字符串。
    每次写入都会覆盖旧值。
    """
    original_forward = module.forward

    async def wrapped_forward(pipeline_data: Dict[str, Any]) -> Dict:
        request_id = pipeline_data.get("id")
        if not request_id:
            logger.warning(f"模块 {stage_name}: pipeline_data 中缺少 'id'。无法持久化状态。")
            return await original_forward(pipeline_data)

        # 先执行原始模块，再更新状态，确保状态与数据同步
        output_data = await original_forward(pipeline_data)

        if _redis_client:
            redis_key = f"{PIPELINE_STATUS_PREFIX}{request_id}"
            try:
                # 将 stage_name 写入 Redis
                await _redis_client.set(redis_key, stage_name, ex=7200)  # 使用 setex 合并 set 和 expire
                logger.debug(f"已将 ID: {request_id} 的状态更新为 '{stage_name}' 到 Redis 键: {redis_key}")
            except Exception as e:
                logger.error(f"持久化 ID: {request_id} 的 '{stage_name}' 状态失败: {e}", exc_info=True)
        else:
            logger.warning(f"Redis 客户端不可用。无法持久化 ID: {request_id} 的 '{stage_name}' 状态。")

        # 确保 output_data 被返回
        return output_data

    class WrappedModule(Module):
        def __init__(self, original_module_name: str):
            super().__init__()
            self.__name__ = original_module_name

        async def forward(self, pipeline_data: Dict[str, Any]) -> Dict:
            return await wrapped_forward(pipeline_data)

    return WrappedModule(f"{stage_name}_persisted")


class QAPipeline(Sequential):
    def __init__(self, config, redis_client: aioredis.Redis, worker_num=1):
        worker_num = worker_num * 10
        self.config = config
        self.redis_client = redis_client

        self.intent_understander = IntentUnderstander(self.config["intent"])
        self.search_knowledge = SearchKnowledge(self.config["similar_query"])
        self.answer_generator = AnswerGeneration(self.config["answer_generation"],self.redis_client)
        self.answer_evaluator = QualityAssurance(self.config["quality_answer"])
        self.answer_corrector = Answer_Correct(self.config["correct_answer"], self.redis_client)

        self._results_buffer: Dict[str, Dict] = {}
        self.dict_semaphore = Semaphore(1)

        self.persisted_intent_module = create_persisted_module_wrapper(self.intent_understander, "intent_output")
        self.persisted_search_module = create_persisted_module_wrapper(self.search_knowledge, "knowledge_output")
        self.persisted_generate_module = create_persisted_module_wrapper(self.answer_generator, "answer_output")
        self.persisted_evaluate_module = create_persisted_module_wrapper(self.answer_evaluator, "quality_output")
        self.persisted_correct_module = create_persisted_module_wrapper(self.answer_corrector, "correct_output")

        self.intent_node = Node(self.persisted_intent_module, worker_num=worker_num)
        self.search_node = Node(self.persisted_search_module, worker_num=worker_num)
        self.generate_node = Node(self.persisted_generate_module, worker_num=worker_num)
        self.evaluate_node = Node(self.persisted_evaluate_module, worker_num=worker_num)

        # 用于推送初步答案到 Redis 的决策节点
        # self.redis_streamer_node = Node(self.decide_and_push_to_redis, worker_num=worker_num)

        self.correct_node = Node(self.persisted_correct_module, worker_num=worker_num)
        self.collect_node = Node(self.collect_to_sink, worker_num=worker_num, queue_size=5, no_output=True)

        # 调整流水线结构，插入新的 redis_streamer_node
        super().__init__([
            self.intent_node,
            self.search_node,
            self.generate_node,
            self.evaluate_node,
            # self.redis_streamer_node,  # <--- 新节点
            self.correct_node,
            self.collect_node
        ], head=self.intent_node, tail=self.collect_node)

    # 决策和推送到 Redis 的新方法
    # async def decide_and_push_to_redis(self, pipeline_data: Dict[str, Any]) -> Dict[str, Any]:
    #     """
    #     检查评估结果。如果答案质量好，就将其写入 Redis List 并更新状态为 STREAMING。
    #     """
    #     request_id = pipeline_data.get("id")
    #     if not request_id or not self.redis_client:
    #         return pipeline_data
    #
    #     evaluation_result = pipeline_data.get("quality_output", {})
    #     is_correct = evaluation_result.get("is_correct", False)
    #     preliminary_answer = pipeline_data.get("answer_output", {}).get("answer")
    #
    #     if is_correct and preliminary_answer:
    #         stream_key = f"{PIPELINE_STREAM_PREFIX}{request_id}"
    #         status_key = f"{PIPELINE_STATUS_PREFIX}{request_id}"
    #         try:
    #             # 1. 将初步答案推入列表
    #             await self.redis_client.rpush(stream_key, preliminary_answer)
    #
    #             # 2. 更新状态，告诉客户端可以来取数据了
    #             await self.redis_client.set(status_key, "STREAMING")
    #
    #             # 3. 为两个键设置或刷新过期时间
    #             await self.redis_client.expire(stream_key, 3600)
    #             await self.redis_client.expire(status_key, 3600)
    #
    #             logger.info(f"ID: {request_id} 的初步答案已写入 Redis。")
    #         except Exception as e:
    #             logger.error(f"ID: {request_id} 写入 Redis 失败: {e}", exc_info=True)
    #
    #     # 无论如何，都将数据原封不动地传递给下一个节点
    #     return pipeline_data

    # 改造 collect_to_sink 以支持最终答案的写入
    async def collect_to_sink(self, x: Dict):
        request_id = x.get("id")
        if not request_id:
            logger.warning("流水线输出缺少 'id' 用于结果收集。无法更新整体 Redis 状态。")
            return

        # 1. 将最终结果放入内存 buffer，供gRPC方法获取
        async with self.dict_semaphore:
            self._results_buffer[request_id] = x
            logger.info(f"流水线已完成 ID: {request_id} 的处理")

        # 2. 将最终结果和状态写入 Redis
        if self.redis_client:
            status_key = f"{PIPELINE_STATUS_PREFIX}{request_id}"
            # stream_key = f"{PIPELINE_STREAM_PREFIX}{request_id}"

            # final_answer = x.get("correct_output", {}).get("final_answer")
            # if final_answer:
            #     await self.redis_client.rpush(stream_key, final_answer)
            #     await self.redis_client.expire(stream_key, 3600)

            try:
                # 更新最终状态为 COMPLETED
                status_string = "COMPLETED"
                await self.redis_client.set(status_key, status_string)
                await self.redis_client.expire(status_key, 3600)  # 刷新过期时间
                logger.debug(f"Redis 整体流水线状态已更新为 {status_string}，针对 {status_key}")
            except Exception as e:
                logger.error(f"更新 Redis 整体状态失败，针对 {status_key} ({status_string}): {e}",
                             exc_info=True)


class QAPipelineServiceServicer(qa_service_pb2_grpc.QAPipelineServiceServicer):
    def __init__(self):
        self.qa_pipeline = QAPipeline(flowsettings.LLM_CONFIG, _redis_client)
        self.qa_pipeline.start()

    async def ProcessQuestion(self, request, context):
        request_id = request.id
        question_text = request.question

        messages_list = []
        for msg in request.messages:
            role_name = qa_service_pb2.RoleType.Name(msg.role_type)
            messages_list.append({
                "role_type": role_name,
                "content": msg.content,
                "content_type": msg.content_type
            })

        logger.info(f"收到 gRPC 请求，ID: {request_id}, 问题: {question_text}")
        logger.debug(f"收到的消息: {messages_list}")

        if _redis_client:
            redis_key_overall_status = f"{PIPELINE_STATUS_PREFIX}{request_id}"
            status_string = "PROCESSING"
            try:
                await _redis_client.setex(redis_key_overall_status, 7200, status_string)
                logger.debug(f"Redis 初始状态已设置为 {status_string}，针对 {redis_key_overall_status}")
            except Exception as e:
                logger.error(f"更新 Redis 初始状态失败，针对 {redis_key_overall_status} ({status_string}): {e}",
                             exc_info=True)

        start_time = time.time()

        input_data = {
            "id": request_id,
            "question": question_text,
            "messages": messages_list
        }
        await self.qa_pipeline.put(input_data)

        max_wait_time = 150
        check_interval = 0.5
        elapsed_time = 0
        result_data = None

        while elapsed_time < max_wait_time:
            async with self.qa_pipeline.dict_semaphore:
                if request_id in self.qa_pipeline._results_buffer:
                    result_data = self.qa_pipeline._results_buffer.pop(request_id)
                    break
            await asyncio.sleep(check_interval)
            elapsed_time += check_interval

        if result_data is None:
            logger.error(f"流水线未在预期时间内为 ID: {request_id} 生成输出。")
            context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
            context.set_details("处理超时。")

            if _redis_client:
                redis_key_overall_status = f"{PIPELINE_STATUS_PREFIX}{request_id}"
                status_string = "TIMEOUT"
                try:
                    await _redis_client.setex(redis_key_overall_status, 60, status_string)
                    logger.debug(f"Redis 整体状态已更新为 {status_string}，针对 {redis_key_overall_status}")
                except Exception as e:
                    logger.error(f"更新 Redis 超时状态失败，针对 {redis_key_overall_status} ({status_string}): {e}",
                                 exc_info=True)
            return qa_service_pb2.ProcessQuestionResponse()

        end_time = time.time()
        duration = end_time - start_time

        response_messages = []

        # 从 result_data 中获取最终答案
        final_answer_content = result_data.get("correct_output", {}).get("final_answer", "未能生成最终答案。")
        if final_answer_content:
            response_messages.append(
                qa_service_pb2.MessageDetail(
                    role_type=qa_service_pb2.ASSISTANT,
                    content=final_answer_content,
                    content_type="text"
                )
            )
        result_json_string = json.dumps(result_data, ensure_ascii=False, indent=2)
        logger.info(f"gRPC 请求 ID {request_id} 的总处理时间: {duration:.2f} 秒")
        logger.debug(f"准备返回的结构化消息数量: {len(response_messages)}")

        return qa_service_pb2.ProcessQuestionResponse(
            result_json=result_json_string,
            duration_seconds=duration,
            messages=response_messages
        )


async def serve():
    global _redis_client

    try:
        _redis_client = aioredis.Redis(
            host=flowsettings.REDIS_HOST,
            port=flowsettings.REDIS_PORT,
            password=getattr(flowsettings, "REDIS_PASSWORD", None),
            db=flowsettings.REDIS_DB,
            decode_responses=True
        )
        await _redis_client.ping()
        logger.info("成功连接到异步 Redis。")
    except Exception as e:
        logger.error(f"连接异步 Redis 失败: {e}", exc_info=True)
        _redis_client = None

    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = QAPipelineServiceServicer()
    qa_service_pb2_grpc.add_QAPipelineServiceServicer_to_server(servicer, server)

    SERVICE_NAMES = (
        qa_service_pb2.DESCRIPTOR.services_by_name['QAPipelineService'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    logger.info("gRPC Server Reflection 已启用。")

    listen_addr = '[::]:50054'
    server.add_insecure_port(listen_addr)
    logger.info(f"在 {listen_addr} 启动 gRPC 服务器")

    qa_pipeline_instance = servicer.qa_pipeline
    pipeline_analyser = PipelineAnalyser()
    pipeline_analyser.register(qa_pipeline_instance)
    monitor = Monitor(report_interval=300)
    monitor.register(pipeline_analyser)
    monitor.start()
    logger.info("PipelineAnalyser 和 Monitor 已启动。")

    await server.start()
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        logger.info("gRPC server cancelled.")
    finally:
        logger.info("PipelineAnalyser 和 Monitor 已停止。")
        if _redis_client:
            await _redis_client.close()
            logger.info("异步 Redis 连接已关闭。")


if __name__ == "__main__":
    asyncio.run(serve())
