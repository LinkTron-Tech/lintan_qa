import json
from volcengine.auth.SignerV4 import SignerV4
from volcengine.base.Request import Request
from volcengine.Credentials import Credentials
from setting import settings as flowsettings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type,wait_fixed
from httpx import Timeout
import httpx
from typing import Optional,Dict,Any,Union,List
import logging
import copy
logger = logging.getLogger(__name__)

CUSTOM_TIMEOUT = Timeout(10.0)

class KnowledgeBaseClient:
    def __init__(self):
        self.ak = flowsettings.KN_CONFIG["AK"]
        self.sk = flowsettings.KN_CONFIG["SK"]
        self.account_id = flowsettings.KN_CONFIG["ACCOUND_ID"]
        self.collection_name = flowsettings.KN_CONFIG["COLLECTION_NAME"]
        self.project_name = flowsettings.KN_CONFIG["PROJECT_NAME"]
        self.knowledge_base_domain = flowsettings.KN_CONFIG["KNOWLEDGE_BASE_DOMAIN"]
        self.limit =   flowsettings.KN_CONFIG["KNOWLEDGE_LIMIT"]
        self.dense_weight = flowsettings.KN_CONFIG["KNOWLEDGE_DENSEWEIGHT"]
        self.rerank_switch = flowsettings.KN_CONFIG["KNOWLEDGE_RERANK_SWITCH"]
        self.rerank_model = flowsettings.KN_CONFIG["KNOWLEDGE_RERANK_MODEL"]
        self.retrieve_count = flowsettings.KN_CONFIG["KNOWLEDGE_RETRIEVE_COUNT"]
        self.filter_cond = flowsettings.KN_CONFIG["KNOWLEDGE_FILTER_COND"]

    def _prepare_request(self, method, path, params=None, data=None, doseq=0):
        if params:
            for key in params:
                if isinstance(params[key], (int, float, bool)):
                    params[key] = str(params[key])
                elif isinstance(params[key], list):
                    if not doseq:
                        params[key] = ",".join(params[key])

        r = Request()
        r.set_shema("http")
        r.set_method(method)
        r.set_connection_timeout(10)
        r.set_socket_timeout(10)

        mheaders = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Host": self.knowledge_base_domain,
            "V-Account-Id": self.account_id,
        }
        r.set_headers(mheaders)

        if params:
            r.set_query(params)

        r.set_host(self.knowledge_base_domain)
        r.set_path(path)

        if data is not None:
            r.set_body(json.dumps(data))

        credentials = Credentials(self.ak, self.sk, "air", "cn-north-1")
        SignerV4.sign(r, credentials)
        return r

    def _parse_filter_cond(self, filter_cond: Optional[Dict[str, Any]] = None,
                           geographic_scope: Optional[Union[str, List[str]]] = None) -> Optional[Dict[str, Any]]:
        """
        解析并合并过滤条件，对多个地理范围总是使用 OR 逻辑。
        """
        all_conditions = []
        top_level_op = "and"  

        if filter_cond:
            temp_filter_cond = copy.deepcopy(filter_cond)
            top_level_op = temp_filter_cond.get("Cond_relation", "and").lower()
            
            specific_conds = temp_filter_cond.get("Specific_cond", [])
            for item in specific_conds:
                op_type = item.get("is_Contain")
                field_name = item.get("Field")
                conditions_value = item.get("conds")
                if not all([op_type, field_name, conditions_value is not None]):
                    continue
                
                all_conditions.append({
                    "op": op_type,
                    "field": field_name,
                    "conds": conditions_value if isinstance(conditions_value, list) else [conditions_value]
                })

        # 2. 处理并准备 geographic_scope 条件
        if geographic_scope:
            scope_list = [geographic_scope] if isinstance(geographic_scope, str) else geographic_scope
            scope_list = [scope for scope in scope_list if scope] if scope_list else []

            if len(scope_list) == 1:
                geo_condition = {
                    "op": "must",
                    "field": "地域归属",
                    "conds": [scope_list[0]]
                }
                all_conditions.append(geo_condition)
            elif len(scope_list) > 1:
                geo_or_block = {
                    "op": "or",
                    "conds": [
                        {"op": "must", "field": "地域归属", "conds": [scope]}
                        for scope in scope_list
                    ]
                }
                all_conditions.append(geo_or_block)

        if not all_conditions:
            return None
        if len(all_conditions) == 1 and "op" in all_conditions[0] and "conds" in all_conditions[0]:
            return all_conditions[0]
        
        return {
            "op": top_level_op,
            "conds": all_conditions
        }


    @retry(
    retry=retry_if_exception_type(httpx.ReadTimeout),
    stop=stop_after_attempt(3),
    wait=wait_fixed(0.5), 
    )
    async def search_knowledge(self, query: str,geographic_scope) -> list:
        method = "POST"
        path = "/api/knowledge/collection/search_knowledge"

        request_params = {
            "project": self.project_name,
            "name": self.collection_name,
            "query": query,
            "limit": self.limit,
            "pre_processing": {
                "need_instruction": True,
                "return_token_usage": True,
                "messages": [
                    {"role": "system", "content": ""},
                    {"role": "user"}
                ]
            },
            "dense_weight": self.dense_weight,
            "post_processing": {
                "get_attachment_link": True,
                "rerank_only_chunk": False,
                "rerank_switch": self.rerank_switch,
                "rerank_model": self.rerank_model,
                "retrieve_count": self.retrieve_count
            },
        }

        doc_filter = self._parse_filter_cond(self.filter_cond,geographic_scope)

        if doc_filter:
            request_params["query_param"] = {
                "doc_filter": doc_filter
            }

        print("request_params:",request_params)

        info_req = self._prepare_request(method=method, path=path, data=request_params)


        async with httpx.AsyncClient(timeout=CUSTOM_TIMEOUT) as client:
            rsp = await client.request(
                method=info_req.method,
                url=f"http://{self.knowledge_base_domain}{info_req.path}",
                headers=info_req.headers,
                content=info_req.body
            )

        response_data = json.loads(rsp.text)
        if "data" not in response_data or "result_list" not in response_data["data"]:
            logger.info(f"知识库返回响应格式异常 - {response_data}")
            return []

        result_list = response_data["data"]["result_list"]
        kn_list = []
        for item in result_list:
            doc_name = item.get("doc_info", {}).get("doc_name")
            point_id = item.get("point_id")
            score = item.get("score")

            # 使用列表推导式过滤字段
            filtered_fields = [
                field for field in item.get("table_chunk_fields", [])
                if field.get("field_name") not in ["内容效力状态", "文件类型", "关键词"]
            ]

            temp = {
                "doc_name": doc_name,
                "table_chunk_fields": filtered_fields,
                "point_id": point_id,
                "score": score
            }
            kn_list.append(temp)
        return kn_list


if __name__ == "__main__":
    import asyncio

    async def main():
        client = KnowledgeBaseClient()
        query = "哪些生产建设项目需要编报水土保持方案？"
        results = await client.search_knowledge(query=query,geographic_scope=["湖北省武汉市","湖北省"])
        print(json.dumps(results, indent=2, ensure_ascii=False))


    asyncio.run(main())