"""
模型接口基类
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

class ModelInterface(ABC):
    """模型接口基类，定义了模型调用的基本方法"""
    
    @abstractmethod
    def __init__(self, model_name: str, **kwargs):
        """初始化模型
        
        Args:
            model_name: 模型名称
            **kwargs: 模型初始化参数
        """
        pass
    
    @abstractmethod
    def generate(self, inputs: List[str], **kwargs) -> List[str]:
        """生成文本
        
        Args:
            inputs: 输入文本列表
            **kwargs: 生成参数
            
        Returns:
            生成的文本列表
        """
        pass
    
    @abstractmethod
    def embeddings(self, inputs: List[str], **kwargs) -> List[List[float]]:
        """生成文本嵌入
        
        Args:
            inputs: 输入文本列表
            **kwargs: 嵌入生成参数
            
        Returns:
            文本嵌入列表
        """
        pass
    
    @abstractmethod
    def classify(self, inputs: List[str], labels: List[str], **kwargs) -> List[Dict[str, Any]]:
        """文本分类
        
        Args:
            inputs: 输入文本列表
            labels: 分类标签列表
            **kwargs: 分类参数
            
        Returns:
            分类结果列表，每个结果包含标签和置信度
        """
        pass
    
    @abstractmethod
    def close(self):
        """关闭模型资源"""
        pass