# import os, sys
# # 把项目根目录加入 path
# ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
# sys.path.insert(0, ROOT)
from importlib import import_module
from async_d.analyser import SingletonMeta

from setting import settings as flowsettings
import logging
logger = logging.getLogger(__name__)

class PromptManager(metaclass = SingletonMeta):
    def __init__(self):
        self.DEFAULT_LANGUAGE = "zh"
        self.prompt = {}
        self._load_prompts()
        try:
            self.langs = getattr(flowsettings, "LANGUAGES")
        except AttributeError:
            logger.error("No languages found in flowsettings, using default language 'zh'.")
            raise ValueError("No languages found in flowsettings.")

    def _load_prompts(self):
        """Load prompts based on language setting"""
        try:
            self.prompt['zh'] = import_module(f"{__package__}.prompts_zh")
            self.prompt['en'] = import_module(f"{__package__}.prompts_en")
        except ImportError:
            self.prompt[self.DEFAULT_LANGUAGE] = import_module(f"{__package__}.prompts_{self.DEFAULT_LANGUAGE}")
            logger.warning(
                f"Unsupported language!"
            )

    def get_prompt(self, name, lang:str)->str:
              
        user_lang = self.langs[lang][ f"{ self.langs[lang]['prompt_lang'] }_name"]
        prompt_module = self.prompt.get(self.langs[lang]['prompt_lang']) 
        try:
            prompt = getattr(prompt_module, name)    
        except AttributeError:
            logger.error(f"No prompt named '{name}' for language '{lang}'")
            raise
        return prompt, user_lang