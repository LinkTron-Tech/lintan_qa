import re
import json


def parse_llm_json_output(llm_output) -> dict:
    """
    兼容传入 str 或 dict。
    """
    if isinstance(llm_output, dict):
        return llm_output
    if not isinstance(llm_output, str):
        raise TypeError(f"Expected str or dict, got {type(llm_output)}")

    # 尝试直接解析整个字符串
    try:
        return json.loads(llm_output)
    except json.JSONDecodeError:
        pass

    # 查找 ```json ... ``` 块
    pattern = r"```json\s*([\s\S]*?)\s*```"
    matches = re.findall(pattern, llm_output)
    if not matches:
        raise ValueError("未找到合法的 JSON 内容或 ```json ... ``` 块")
    last_json_str = matches[-1]
    try:
        return json.loads(last_json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"解析 ```json``` 块中的 JSON 失败: {e}")



# def parse_llm_though_output(llm_output_str: str) -> tuple[str, str]:
#     # 匹配整个 <thought>... </thought> 块及其内部内容
#     # ([\s\S]*?) 捕获非贪婪模式下的所有字符，包括换行符
#     pattern = r"<thought>\s*([\s\S]*?)\s*</thought>"

#     # 使用 re.search 查找第一个匹配项，以获取其在字符串中的位置
#     match = re.search(pattern, llm_output_str)

#     if not match:
#         raise ValueError("未找到合法的<thought> 块")

#     # 提取思考过程内容，它位于匹配组 1
#     thought_str = match.group(1).strip()

#     # 使用 re.sub 移除整个匹配到的 <thought> 块，得到剩余的文本
#     content_str = re.sub(pattern, "", llm_output_str, count=1).strip()

#     return thought_str, content_str

def parse_llm_though_output(llm_output_str: str) -> tuple[str, str]:
    """
    尝试从LLM的输出中解析 <thought> 块。

    Args:
        llm_output_str: 大模型返回的原始字符串。

    Returns:
        一个元组 (thought_str, content_str)。
        - 如果成功解析：返回提取出的思考过程和剩余的答案内容。
        - 如果解析失败：返回一个默认的思考过程字符串和完整的原始输出作为答案。
    """
    # 确保输入是字符串类型
    if not isinstance(llm_output_str, str):
        llm_output_str = str(llm_output_str) # 做一次强制转换以防意外

    # 匹配 <thought>...</thought> 块，允许内部有任何字符（包括换行）
    pattern = r"<thought>([\s\S]*?)</thought>"
    match = re.search(pattern, llm_output_str)

    # 如果找到了匹配项
    if match:
        # 提取思考过程内容
        thought_str = match.group(1).strip()
        
        content_str = re.sub(pattern, "", llm_output_str, count=1).strip()
        
        if not content_str:
            content_str = thought_str
            thought_str = "模型将思考过程和答案合并输出。" 

        return thought_str, content_str
    
    else:
        # 提供一个默认的思考过程
        default_thought = "模型未按预期格式提供独立的思考过程。"
        
        # 将完整的原始输出作为答案内容返回
        content_str = llm_output_str.strip()
        
        return content_str, content_str


# def format_snippet_data(data_list):
#     """
#     将包含知识切片信息的字典列表格式化为指定字符串。
#     Args:
#         data_list (list): 包含知识切片信息的字典列表。
#     Returns:
#         str: 拼接后的格式化字符串。
#     """
#     formatted_output = []
#     for i, item in enumerate(data_list):
#         section_start = f"section {i + 1} start:"
#         fields_str = []

#         for field in item['table_chunk_fields']:
#             field_name = field['field_name']
#             field_value = field['field_value']
#             fields_str.append(f"{field_name}:{field_value}")

#         section_content = " ".join(fields_str)
#         section_end = f"section {i + 1} ends."

#         formatted_output.append(f"{section_start} {section_content} {section_end}")


#     return "".join(formatted_output)

def format_snippet_data(data_list):
    """
    将包含知识切片信息的字典列表格式化为 "key:value" 字符串。
    """
    # 定义一个要排除的键的集合，这些是上下文不需要的元数据
    EXCLUDED_KEYS = {'rerank_score', 'id', '切片ID', '来源文件名', "关键词"}
    
    formatted_output = []

    for i, item in enumerate(data_list):
        section_number = i + 1
        section_start = f"section {section_number} start:"
        section_end = f"section {section_number} ends."

        # 用于存储 "key:value" 字符串的列表
        key_value_pairs = []
        
        # 遍历字典的键值对
        for key, value in item.items():
            # 如果键不在排除列表中，并且值不为空，则进行处理
            if key not in EXCLUDED_KEYS and value and str(value).strip() != '':
                # 清理值中的换行符和首尾空格
                clean_value = str(value).replace('\n', ' ').strip()
                # 格式化为 "key:value" 字符串并添加到列表中
                key_value_pairs.append(f"{key}:{clean_value}")
        
        # 使用单个空格将所有的 "key:value" 对连接起来
        section_content = " ".join(key_value_pairs)
        
        # 拼接最终的 section 字符串
        formatted_output.append(f"{section_start} {section_content} {section_end}")

    # 使用换行符将所有 section 连接成最终的完整字符串
    return "".join(formatted_output)




def normalize_span_citations(text: str) -> str:
    chinese_to_num = {
        '一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
        '六': '6', '七': '7', '八': '8', '九': '9', '十': '10'
    }
    
    def replacer(match):
        # match.group(0) 是完整的 <span>...</span> 标签
        # match.group(1) 是标签内部的内容
        original_span_tag = match.group(0)
        content = match.group(1).strip()
        
        # 步骤 1: 替换中文数字
        if content in chinese_to_num:
            content = chinese_to_num[content]

        # 步骤 2: 解析内容并拆分为数字列表
        numbers: list[str] = []
        is_valid_citation_format = False # 追踪是否是可识别的引文格式
        
        if content.isdigit():
            # 优先级最高：处理单个阿拉伯数字（包括经过中文替换后的数字）
            numbers = [content]
            is_valid_citation_format = True
            
        elif ',' in content:
            # 处理合并引用，如 "1,2,3"
            numbers.extend(c.strip() for c in content.split(',') if c.strip().isdigit())
            # 只有当成功提取出至少一个数字时，才认为是有效引文
            if numbers:
                is_valid_citation_format = True
            
        elif '-' in content:
            # 处理范围引用，如 "3-5"
            try:
                start, end = map(int, content.split('-'))
                if start <= end:
                    numbers.extend(str(i) for i in range(start, end + 1))
                    is_valid_citation_format = True
            except ValueError:
                pass # 范围解析失败，不认为是引文

        # 只有识别出有效的引文格式并且成功解析出数字时，才继续格式化
        if not is_valid_citation_format or not numbers:
            # 原样返回完整的、带有属性的 <span> 标签 (处理非引文内容或解析失败的)
            return original_span_tag
        
        # 步骤 3 & 4: 使用 <span>x</span> 格式进行统一
        return "".join(f"<span>{num}</span>" for num in numbers)

    return re.sub(r'(?s)<span>(.*?)</span>', replacer, text)