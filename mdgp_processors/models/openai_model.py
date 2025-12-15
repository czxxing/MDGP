"""
OpenAI兼容模型实现，用于调用各种支持OpenAI API的开源模型库
"""

import json
import requests
from typing import Any, Dict, List, Optional
from .model_interface import ModelInterface
from .model_factory import model_factory

# 尝试导入openai库
try:
    import openai
    _openai_available = True
except ImportError:
    _openai_available = False

class OpenAIModel(ModelInterface):
    """OpenAI兼容模型实现"""
    
    def __init__(self, model_name: str, api_key: Optional[str] = None, base_url: Optional[str] = None, **kwargs):
        """初始化OpenAI兼容模型
        
        Args:
            model_name: 模型名称
            api_key: API密钥
            base_url: API基础URL
            **kwargs: 其他参数
        """
        super().__init__()
        self.model_name = model_name
        self.api_key = api_key
        self.base_url = base_url
        self.kwargs = kwargs
        
        # 初始化OpenAI客户端
        if _openai_available:
            self.client = openai.OpenAI(
                api_key=api_key,
                base_url=base_url,
                **kwargs
            )
        else:
            self.client = None
            self.session = requests.Session()
    
    def generate(self, inputs: List[str], **kwargs) -> List[str]:
        """生成文本
        
        Args:
            inputs: 输入文本列表
            **kwargs: 生成参数
            
        Returns:
            生成的文本列表
        """
        generated_texts = []
        
        if self.client is not None:
            # 使用OpenAI客户端
            for input_text in inputs:
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": input_text}],
                    **kwargs
                )
                generated_texts.append(response.choices[0].message.content)
        else:
            # 使用HTTP请求
            url = f"{self.base_url}/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            for input_text in inputs:
                payload = {
                    "model": self.model_name,
                    "messages": [{"role": "user", "content": input_text}],
                    **kwargs
                }
                
                response = self.session.post(url, headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()
                generated_texts.append(result["choices"][0]["message"]["content"])
        
        return generated_texts
    
    def embeddings(self, inputs: List[str], **kwargs) -> List[List[float]]:
        """生成文本嵌入
        
        Args:
            inputs: 输入文本列表
            **kwargs: 嵌入生成参数
            
        Returns:
            文本嵌入列表
        """
        if self.client is not None:
            # 使用OpenAI客户端
            response = self.client.embeddings.create(
                model=self.model_name,
                input=inputs,
                **kwargs
            )
            return [embedding.embedding for embedding in response.data]
        else:
            # 使用HTTP请求
            url = f"{self.base_url}/v1/embeddings"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.model_name,
                "input": inputs,
                **kwargs
            }
            
            response = self.session.post(url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return [item["embedding"] for item in result["data"]]
    
    def classify(self, inputs: List[str], labels: List[str], **kwargs) -> List[Dict[str, Any]]:
        """文本分类
        
        Args:
            inputs: 输入文本列表
            labels: 分类标签列表
            **kwargs: 分类参数
            
        Returns:
            分类结果列表，每个结果包含标签和置信度
        """
        classifications = []
        
        if self.client is not None:
            # 使用OpenAI客户端进行零样本分类
            for input_text in inputs:
                prompt = f"将以下文本分类到这些标签中：{', '.join(labels)}\n\n文本：{input_text}\n\n分类结果："
                response = self.client.completions.create(
                    model=self.model_name,
                    prompt=prompt,
                    **kwargs
                )
                
                # 解析结果
                result_text = response.choices[0].text.strip()
                classification = {"labels": [result_text], "scores": [1.0]}
                classifications.append(classification)
        else:
            # 使用HTTP请求
            url = f"{self.base_url}/v1/completions"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            for input_text in inputs:
                prompt = f"将以下文本分类到这些标签中：{', '.join(labels)}\n\n文本：{input_text}\n\n分类结果："
                payload = {
                    "model": self.model_name,
                    "prompt": prompt,
                    **kwargs
                }
                
                response = self.session.post(url, headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()
                
                # 解析结果
                result_text = result["choices"][0]["text"].strip()
                classification = {"labels": [result_text], "scores": [1.0]}
                classifications.append(classification)
        
        return classifications
    
    def close(self):
        """关闭模型资源"""
        if hasattr(self, "session"):
            self.session.close()

# 注册OpenAI模型
model_factory.register_model("openai", OpenAIModel)