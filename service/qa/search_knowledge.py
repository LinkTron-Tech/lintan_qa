from typing import Dict, Any
import logging
from tenacity import retry, stop_after_attempt, after_log, retry_if_exception_type
import asyncio
import json
from datetime import datetime, timedelta
from service.prompts import prompt_manager
from service.utils.llm_clients import create_ai_client
from service.utils.qrant_kn import AdvancedSearcher
from service.utils.process_str import parse_llm_json_output
from base_method.module import Neuron, Module
from setting import settings as flowsettings

logger = logging.getLogger(__name__)

RETRIEVAL_LIMIT = flowsettings.KNOWLEDGE_CONFIGS["RETRIEVAL_LIMIT"]
RERANK_TOP_N = flowsettings.KNOWLEDGE_CONFIGS["RERANK_TOP_N"]
FINAL_TOP_N = flowsettings.KNOWLEDGE_CONFIGS["FINAL_TOP_N"]


class SearchKnowledge(Module):
    """
    执行知识库召回、去重、排序和初步筛选。
    返回最终精选的知识切片列表。
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.searchKnNeuron = SearchKnNeuron(config["neuron"])
        logger.info(f"[SearchKnowledge] Initialized with model: {config['neuron']['model']}")


    async def forward(self, pipeline_data: Dict[str, Any]) -> Dict:
        """
        根据意图和问题执行知识库召回。
        pipeline_data 预期包含 'id', 'question', 'messages', 'intent'
        """

        request_id = pipeline_data.get("id")
        question = pipeline_data.get("question")
        messages = pipeline_data.get("messages", [])  # 确保传递 messages
        intent_result = pipeline_data.get("intent")  # 从上一个模块获取意图理解结果

        if not intent_result.get("is_relevant_topic"):
            logger.error(f"SearchKnowledge received invalid or missing 'intent' data for ID: {request_id}")
            # 返回错误并保留所有原始数据
            pipeline_data["knowledge"] = {"snippets": ""}
            pipeline_data["error"] = pipeline_data.get("error", "") + "Missing or invalid intent data."
            return pipeline_data

        is_relevant = intent_result.get("is_relevant_topic", False)

        try:
            # searchKnNeuron 接收整个 intent_result，包含 question
            kn_snippets = await self.searchKnNeuron(pipeline_data)  # 传递整个 pipeline_data

            logger.info(f"ID: {request_id}, 知识召回与筛选的结果：{kn_snippets}")

            # 在 pipeline_data 中添加知识库召回结果
            pipeline_data["knowledge"] = {
                "snippets": kn_snippets
            }
        except Exception as e: 
            logger.error(f"ID: {request_id}, 知识召回失败: {e}", exc_info=True)
            pipeline_data["knowledge"] = {
                "snippets": ""
            }
            pipeline_data["error"] = pipeline_data.get("error", "") + f"Knowledge search failed: {e}"

        return pipeline_data 


class SearchKnNeuron(Neuron):

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
    async def forward(self, pipeline_data: Dict[str, Any], lang='zh') -> list  :
        # 从 pipeline_data 中提取 intent 和 rewrite question
        intent_result = pipeline_data.get("intent")
        request_id = pipeline_data.get("id")


        if not intent_result:
            logger.error(f"SearchKnNeuron missing intent or question in input_data for ID: {request_id}")
            raise ValueError("Missing intent or question in input data for SearchKnNeuron.")

        
        # 从 intent 中获取改写后的问题
        rewritten_question = intent_result.get("rewritten_question")
        if not rewritten_question:
            logger.error(f"ID: {request_id}, SearchKnNeuron 缺少 'rewritten_question'。")
            raise ValueError("Missing rewritten_question in intent data.")

        # --- 2. 并行执行查询扩展和过滤器生成 ---
        logger.info(f"ID: {request_id}, 基于 '{rewritten_question}' 生成检索策略...")
        today = datetime.now().strftime('%Y-%m-%d')
        
        query_prompt, _ = self.prompt_manager.get_prompt("QUERY_EXPANSION", lang=lang)
        filter_prompt, _ = self.prompt_manager.get_prompt("FILTER_GENERATION", lang=lang)
        
        query_prompt = query_prompt.format(conversation=rewritten_question)

        filter_prompt = filter_prompt.format(
            current_date=today,
            user_question=rewritten_question,
        )
        
        
        # 并行执行LLM调用
        query_task = self.req_pool.completion(user_message = rewritten_question,system_prompt=query_prompt, response_format=True)

        filter_task = self.req_pool.completion(user_message=filter_prompt, response_format=True)

        query_result, filter_result = await asyncio.gather(query_task, filter_task)

        # 解析LLM的输出
        query_result = parse_llm_json_output(query_result.get("content",""))
        filter_result = parse_llm_json_output(filter_result.get("content",""))

        snippets = await self._retrieve_and_filter_knowledge(query_result, filter_result,rewritten_question)

        return snippets



    async def _retrieve_and_filter_knowledge(self, query_result: dict,filter_result, rewritten_question: str) -> list[dict[Any, Any]]:
        """
        执行知识库召回、去重、排序和初步筛选。
        返回最终精选的知识切片列表。
        """
        logging.info("开始知识库召回与初步筛选。")
        retrieval_queries = query_result.get("queries", [])
        qdrant_filter = filter_result.get("qdrant_filter", {})

        logging.info(f"生成的检索查询: {retrieval_queries}")

        if not retrieval_queries:
            logging.warning("未生成检索查询。将使用原始问题进行知识库搜索。")
            retrieval_queries = [rewritten_question]


        client = AdvancedSearcher()
        tasks = [
            asyncio.to_thread(
                client.search,
                query_item,
                retrieval_limit=RERANK_TOP_N,
                rerank_top_n=RERANK_TOP_N
            )
            for query_item in retrieval_queries
        ]

        all_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 2. 去重
        deduplicated_points = {}
        # 查看召回的原始结果
        logger.info(f"召回阶段原始结果: {all_results}") 
        for result_list in all_results:
            if isinstance(result_list, Exception):
                logging.error(f"一个搜索任务失败: {result_list}")
                continue
            if result_list:
                for point in result_list:
                    # 使用字符串 "id" 作为字典的键
                    point_id = point.get("id")
                    # 确保 point_id 存在且不重复
                    if point_id and point_id not in deduplicated_points:
                        deduplicated_points[point_id] = point

        candidate_points = list(deduplicated_points.values())


        if not candidate_points:
            logging.warning("在召回和重复数据删除后没有找到文件。")
            return []
        
        logging.info(f"Step 2: {len(candidate_points)} 为重新排序准备的唯一文档。")

        # 准备重排所需的数据
        candidate_docs_content = [p.get("原始文本内容", "") for p in candidate_points]

        
        # 3. 调用已有的重排服务
        logging.info(f"Step 3: 根据查询重新排序: '{rewritten_question}'")
        
        final_snippets = await asyncio.to_thread(
            client._rerank_documents,
            query=rewritten_question,
            documents=candidate_docs_content,
            original_points=candidate_points,
            top_n=FINAL_TOP_N
        )

        logging.info(f"--- Reranking complete. Returning {len(final_snippets)} snippets. ---")
        return final_snippets






#
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import asyncio
    import logging

    import json


    async def main():
        # 模拟配置
        config = {
            "neuron": {
                "model": "Gemini-2.5-Flash-Preview",
                "infer_type": "OpenAI"
            }
        }
        searchknowledge = SearchKnowledge(config)
        original_user_input = "哪些生产建设项目需要开展水土保持监理？"

        region_keyword_json = {'is_relevant_topic': True, 'user_intent': '法规查询', 'geographic_scope': '', 'keywords': ['生产建设项目', '水土保持监理'], 'retrieval_queries': ['生产建设项目 水土保持监理 范围', '哪些项目需要开展水土保持监理', '水土保持监理 管理办法 规定 项目']}

        knowledge_result = await searchknowledge.forward({
            "question": original_user_input,
            "intent": region_keyword_json
        })
        logger.info(f'完整切片：\n{knowledge_result["knowledge"]["snippets"]}')
        logger.info(f"point_id_lists：\n{json.dumps(knowledge_result, indent=4, sort_keys=True, ensure_ascii=False)}")
        # logger.info("长度：", len(knowledge_result["knowledge"]["point_ids"]["filtered_point_ids"]))

    asyncio.run(main())