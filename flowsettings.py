import os
from importlib.metadata import version
from pathlib import Path
from decouple import config

this_file = os.path.abspath(__file__)
this_dir = os.path.dirname(this_file)

if os.environ.get("ENV") == "prod":
    # 生产环境
    ENV = config("ENV", default="prod", cast=str)
else:
    # 开发环境
    ENV = config("ENV", default="dev", cast=str)



KNOWLEDGE_CONFIGS = {
    "QDRANT_URL" : "http://localhost:6333",
    "COLLECTION_NAME" : "zero_carbon_kb",
    "EMBEDDING_API_KEY" : "sk-31c931ef057148cfb2b6269a08115836",
    "EMBEDDING_URL" : "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "EMBEDDING_MODEL" : "text-embedding-v4",
    "EMBEDDING_DIMENSIONS" : 2048,
    "VECTOR_SEARCH_FIELD" : "原始文本内容",

    "RERANK_MODEL" : "gte-rerank-v2",
    
    "QD_API_KEY" : "QdR4nt$!23_MyS3cur3Key_2025@AI#xLzT7",
    "RETRIEVAL_LIMIT" : 20,
    "RERANK_TOP_N" : 7,
    "FINAL_TOP_N" : 7

}


LLM_VENDOR_CONFIGS = {
    "deepseek": {
        "base_url": "https://api.deepseek.com",
        "api_key": "sk-ab0224e74e80457e928badd27ff99c22",
        "default_model": "deepseek-chat",
    },
}



## redis配置
if ENV == "prod":
    REDIS_HOST=config("REDIS_HOST", default="127.0.0.1",cast=str)
    REDIS_PORT = config("REDIS_PORT", default=6379, cast=int)
    REDIS_PASSWORD = config("REDIS_PASSWORD", default=None)
    REDIS_DB = config("REDIS_DB", default=13, cast=int)
elif ENV == "dev":
    # 开发环境
    REDIS_HOST=config("REDIS_HOST", default="127.0.0.1",cast=str)
    REDIS_PORT=config("REDIS_PORT", default=6379, cast=int)
    REDIS_PASSWORD = config("REDIS_PASSWORD",default = None)
    REDIS_DB = config("REDIS_DB", default=13, cast=int)




LLM_CONFIG = {

    "model": "deepseek-chat",
    "infer_type": "OpenAI",

    "intent": {
        "neuron":{
            "model": "deepseek-chat",
            "infer_type": "OpenAI",
        }
    },

    "similar_query": {
        "neuron":{
            "model": "deepseek-chat",
            "infer_type": "OpenAI",
        }
    },

    "answer_generation": {
        "neuron":{
            "model": "deepseek-chat",
            "infer_type": "OpenAI",
        }
    },

    "quality_answer": {
        "neuron":{
            "model": "deepseek-chat",
            "infer_type": "OpenAI",
        }
    },

    "correct_answer": {
        "neuron":{
            "model": "deepseek-chat",
            "infer_type": "OpenAI",
        }
    }
}


# 模型输出语言配置和提示词语言配置
LANGUAGES = {
    "en": {
        "en_name" : "English",
        "zh_name" : "英语",
        "prompt_lang" : "en"
    },
    "zh": {
        "en_name" : "Chinese",
        "zh_name" : "中文",
        "prompt_lang" : "zh"
    },
    "ja": {
        "en_name" : "Japanese",
        "zh_name" : "日语",
        "prompt_lang" : "en"
    },
    "es": {
        "en_name" : "Spanish",
        "zh_name" : "西班牙语",
        "prompt_lang" : "en"
    },
    "fr": {
        "en_name" : "French",
        "zh_name" : "法语",
        "prompt_lang" : "en"
    },
    "de": {
        "en_name" : "German",
        "zh_name" : "德语",
        "prompt_lang" : "en"
    },
    "ru": {
        "en_name" : "Russian",
        "zh_name" : "俄语",
        "prompt_lang" : "en"
    },
    "ar": {
        "en_name" : "Arabic",
        "zh_name" : "阿拉伯语",
        "prompt_lang" : "en"
    },
    "pt": {
        "en_name" : "Portuguese",
        "zh_name" : "葡萄牙语",
        "prompt_lang" : "en"
    },
    "it": {
        "en_name" : "Italian",
        "zh_name" : "意大利语",
        "prompt_lang" : "en"
    },
    "ko": {
        "en_name" : "Korean",
        "zh_name" : "韩语",
        "prompt_lang" : "en"
    },
    "vi": {
        "en_name" : "Vietnamese",
        "zh_name" : "越南语",
        "prompt_lang" : "en"
    },
    "th": {
        "en_name" : "Thai",
        "zh_name" : "泰语",
        "prompt_lang" : "en"
    },
    "id": {
        "en_name" : "Indonesian",
        "zh_name" : "印尼语",
        "prompt_lang" : "en"
    },
    "tr": {
        "en_name" : "Turkish",
        "zh_name" : "土耳其语",
        "prompt_lang" : "en"
    },
    "nl": {
        "en_name" : "Dutch",
        "zh_name" : "荷兰语",
        "prompt_lang" : "en"
    },
    "sv": {
        "en_name" : "Swedish",
        "zh_name" : "瑞典语",
        "prompt_lang" : "en"
    },
    "pl": {
        "en_name" : "Polish",
        "zh_name" : "波兰语",
        "prompt_lang" : "en"
    },
    "uk": {
        "en_name" : "Ukrainian",
        "zh_name" : "乌克兰语",
        "prompt_lang" : "en"
    },
    "hi": {
        "en_name" : "Hindi",
        "zh_name" : "印地语",
        "prompt_lang" : "en"
    },
    "bn": {
        "en_name" : "Bengali",
        "zh_name" : "孟加拉语",
        "prompt_lang" : "en"
    },
    "ms": {
        "en_name" : "Malay",
        "zh_name" : "马来语",
        "prompt_lang" : "en"
    },
}