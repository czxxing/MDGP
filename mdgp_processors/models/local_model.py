"""
本地模型实现，用于调用本地部署的模型
"""

import json
import requests
from typing import Any, Dict, List, Optional
from .model_interface import ModelInterface
from .model_factory import model_factory

class LocalModel(ModelInterface):
    """本地模型实现，通过HTTP API调用本地部署的模型"""
    
    def __init__(self, model_name: str, base_url: str = "http://localhost:8000", **kwargs):
        """初始化本地模型
        
        Args:
            model_name: 模型名称
            base_url: 模型服务的基础URL
            **kwargs: 其他参数
        """
        super().__init__()
        self.model_name = model_name
        self.base_url = base_url
        self.session = requests.Session()
        
    def generate(self, inputs: List[str], **kwargs) -> List[str]:
        """生成文本
        
        Args:
            inputs: 输入文本列表
            **kwargs: 生成参数
            
        Returns:
            生成的文本列表
        """
        url = f"{self.base_url}/generate"
        payload = {
            "model": self.model_name,
            "inputs": inputs,
            "parameters": kwargs
        }
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        result = response.json()
        
        return result["generated_texts"]
    
    def embeddings(self, inputs: List[str], **kwargs) -> List[List[float]]:
        """生成文本嵌入
        
        Args:
            inputs: 输入文本列表
            **kwargs: 嵌入生成参数
            
        Returns:
            文本嵌入列表
        """
        url = f"{self.base_url}/embeddings"
        payload = {
            "model": self.model_name,
            "inputs": inputs,
            "parameters": kwargs
        }
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        result = response.json()
        
        return result["embeddings"]
    
    def classify(self, inputs: List[str], labels: List[str], **kwargs) -> List[Dict[str, Any]]:
        """文本分类
        
        Args:
            inputs: 输入文本列表
            labels: 分类标签列表
            **kwargs: 分类参数
            
        Returns:
            分类结果列表，每个结果包含标签和置信度
        """
        url = f"{self.base_url}/classify"
        payload = {
            "model": self.model_name,
            "inputs": inputs,
            "labels": labels,
            "parameters": kwargs
        }
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        result = response.json()
        
        return result["classifications"]
    
    def close(self):
        """关闭模型资源"""
        self.session.close()

# 注册本地模型
model_factory.register_model("local", LocalModel)