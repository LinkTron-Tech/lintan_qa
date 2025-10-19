import os
import json
from http import HTTPStatus
from typing import List, Dict, Optional, Any
from qdrant_client import QdrantClient, models
from openai import OpenAI
from setting import settings as flowsettings
import logging
import dashscope

logger = logging.getLogger(__name__)

# --- [1. 全局配置] ---
QDRANT_URL = flowsettings.KNOWLEDGE_CONFIGS["QDRANT_URL"]
COLLECTION_NAME = flowsettings.KNOWLEDGE_CONFIGS["COLLECTION_NAME"]
EMBEDDING_API_KEY = flowsettings.KNOWLEDGE_CONFIGS["EMBEDDING_API_KEY"]
QD_API_KEY = flowsettings.KNOWLEDGE_CONFIGS["QD_API_KEY"]
EMBEDDING_URL = flowsettings.KNOWLEDGE_CONFIGS["EMBEDDING_URL"]
EMBEDDING_MODEL = flowsettings.KNOWLEDGE_CONFIGS["EMBEDDING_MODEL"]
RERANK_MODEL = flowsettings.KNOWLEDGE_CONFIGS["RERANK_MODEL"]
EMBEDDING_DIMENSIONS = flowsettings.KNOWLEDGE_CONFIGS["EMBEDDING_DIMENSIONS"]
VECTOR_SEARCH_FIELD = flowsettings.KNOWLEDGE_CONFIGS["VECTOR_SEARCH_FIELD"]
dashscope.api_key = EMBEDDING_API_KEY

# --- [2. 可扩展的检索器类] ---

class AdvancedSearcher:
    def __init__(self, qdrant_url=QDRANT_URL, collection_name=COLLECTION_NAME, embedding_api_key=EMBEDDING_API_KEY):
        self.qdrant_client = QdrantClient(url=qdrant_url,api_key=QD_API_KEY)
        self.embedding_client = OpenAI(api_key=embedding_api_key, base_url=EMBEDDING_URL)
        self.collection_name = collection_name
        self.embedding_model, self.embedding_dimensions, self.rerank_model, self.text_search_field =EMBEDDING_MODEL, EMBEDDING_DIMENSIONS, RERANK_MODEL, VECTOR_SEARCH_FIELD
    def _get_embedding(self, text: str) -> list[float]:
        return self.embedding_client.embeddings.create(model=self.embedding_model, input=text, dimensions=self.embedding_dimensions).data[0].embedding
    def _build_qdrant_filter(self, filters: Optional[Dict[str, Any]]) -> Optional[models.Filter]:
        if not filters: return None
        return models.Filter(must=[models.FieldCondition(key=key, match=models.MatchValue(value=value)) for key, value in filters.items()])
    
    # def _rerank_documents(self, query: str, documents: List[str], original_points: List[Any], top_n: int) -> List[Dict[str, Any]]:
        """
        使用重排模型对候选文档进行重排序。
        """


        logger.info(f"\n--- [阶段 3/4] RRF融合后，共 {len(documents)} 条结果送入重排模型 ---")
        try:
            rerank_resp = dashscope.TextReRank.call(
                model=self.rerank_model,
                query=query,
                documents=documents,
                top_n=top_n,
                return_documents=True
            )
        except Exception as e:
            logger.error(f"错误：调用重排模型失败: {e}")
            return []

        final_results = []
        if rerank_resp.status_code == HTTPStatus.OK:
            for result_item in rerank_resp.output.results:
                original_point = original_points[result_item.index]
                final_results.append({
                    "rerank_score": result_item.relevance_score,
                    "id": original_point.id,
                    **original_point.payload
                })
        else:
            logger.info(f"重排失败: {rerank_resp}")
        return final_results

    def _rerank_documents(self, query: str, documents: List[str], original_points: List[Any], top_n: int) -> List[Dict[str, Any]]:
        """
        使用重排模型对候选文档进行重排序。
        可以处理来自Qdrant的ScoredPoint对象或Python字典。
        """
        try:
            rerank_resp = dashscope.TextReRank.call(
                model=self.rerank_model,
                query=query,
                documents=documents,
                top_n=top_n,
                return_documents=True
            )
        except Exception as e:
            logger.error(f"错误：调用重排模型失败: {e}")
            return []

        final_results = []
        if rerank_resp.status_code == HTTPStatus.OK:
            for result_item in rerank_resp.output.results:
                original_point = original_points[result_item.index]

                # 检查 original_point 的类型，以决定如何访问其数据
                if hasattr(original_point, 'id') and hasattr(original_point, 'payload'):
                    # 情况1: 这是一个 Qdrant ScoredPoint 对象
                    result_data = {
                        "rerank_score": result_item.relevance_score,
                        "id": original_point.id,
                        **original_point.payload
                    }
                elif isinstance(original_point, dict):
                    # 情况2: 这是一个字典 (dict)
                    result_data = original_point.copy()  # 复制原始字典
                    result_data["rerank_score"] = result_item.relevance_score # 更新为最新的重排分数
                else:
                    logger.warning(f"跳过未知类型的数据点: {type(original_point)}")
                    continue
                
                final_results.append(result_data)
        else:
            logger.info(f"重排失败: {rerank_resp}")
        
        # 对最终结果按新的 rerank_score 降序排序
        final_results.sort(key=lambda x: x.get("rerank_score", 0.0), reverse=True)

        return final_results

    def search(self, query: str, filters: Optional[Dict[str, Any]] = None, retrieval_limit: int = 20, rerank_top_n: int = 4) -> List[Dict[str, Any]]:
        logger.info(f"--- [阶段 1/4] 开始处理查询: '{query}' ---")
        if filters: logger.info(f"使用过滤器: {filters}")
        try:
            query_vector = self._get_embedding(query)
            logger.info("向量化成功。")
        except Exception as e:
            logger.error(f"错误：查询向量化失败: {e}")
            return []
        qdrant_filter = self._build_qdrant_filter(filters)
        logger.info(f"\n--- [阶段 2/4] 在 Qdrant 中执行混合查询 (召回 {retrieval_limit} 条结果) ---")
        try:
            # 准备第二个 prefetch 查询的过滤器
            text_match_filter_conditions = [
                models.FieldCondition(key=self.text_search_field, match=models.MatchText(text=query))
            ]
            if qdrant_filter and qdrant_filter.must:
                text_match_filter_conditions.extend(qdrant_filter.must)

            # 使用 `prefetch` 执行并行查询
            response = self.qdrant_client.query_points(
                collection_name=self.collection_name,
                prefetch=[
                    # Prefetch 1: 纯向量搜索
                    models.Prefetch(
                        query=query_vector,
                        filter=qdrant_filter,
                        limit=retrieval_limit
                    ),
                    # Prefetch 2: 向量搜索 + 全文匹配
                    models.Prefetch(
                        query=query_vector,
                        filter=models.Filter(must=text_match_filter_conditions),
                        limit=retrieval_limit
                    )
                ],
                # ========================= [修正] =========================
                # `fusion` 参数直接接收字符串 "rrf"。
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                # ============================================================
                limit=retrieval_limit,
                with_payload=True
            )
            logger.info("混合查询成功。")
        except Exception as e:
            logger.error(f"错误：在 Qdrant 中查询失败: {e}")
            return []

        retrieved_points_list = response.points
        if not retrieved_points_list:
            logger.warning("在知识库中没有找到相关结果。")
            return []
        original_points = retrieved_points_list
        documents_to_rerank = [p.payload.get(self.text_search_field, "") for p in original_points]
        logger.info(f"\n--- [阶段 3/4] RRF融合后，共 {len(documents_to_rerank)} 条结果送入重排模型 ---")
        final_results = self._rerank_documents(query, documents_to_rerank, original_points, rerank_top_n)

        logger.info(f"重排完成，返回 Top {len(final_results)} 条结果。")
        return final_results



if __name__ == '__main__':
    searcher = AdvancedSearcher(
        qdrant_url=QDRANT_URL,
        collection_name=COLLECTION_NAME,
        embedding_api_key=EMBEDDING_API_KEY
    )

    logger.info("\n\n=============== [示例 1: 无过滤查询] ===============")
    user_query_1 = "电力设施保护条例第一条"
    results_1 = searcher.search(user_query_1)
    # logger.info(results_1)

    logger.info(f"\n--- [阶段 4/4] ✨ 最终精排结果 (Top {len(results_1)}) ✨ ---")
    for i, result in enumerate(results_1):
        logger.info(f"\n--- [ 结果 {i+1} ] ---")
        logger.info(f"  - Rerank分数: {result['rerank_score']:.4f}")
        logger.info(f"  - Qdrant Point ID: {result['id']}")
        for key, value in result.items():
            logger.info(f"  - {key}: {value if value else 'N/A'}")

    logger.info("\n\n=============== [示例 2: 带过滤查询] ===============")
    user_query_2 = "电力设施保护条例"
    query_filters = {
        "地域归属": "全国",
        "文档类型": ""
    }
    results_2 = searcher.search(user_query_2, filters=query_filters, rerank_top_n=3)
    
    logger.info(f"\n--- [阶段 4/4] ✨ 最终精排结果 (Top {len(results_2)}) ✨ ---")
    for i, result in enumerate(results_2):
        logger.info(f"\n--- [ 结果 {i+1} ] ---")
        logger.info(f"  - Rerank分数: {result['rerank_score']:.4f}")
        logger.info(f"  - Qdrant Point ID: {result['id']}")
        for key, value in result.items():
            logger.info(f"  - {key}: {value if value else 'N/A'}")