"""
模型工厂类，用于创建不同类型的模型实例
"""

from typing import Any, Dict, Optional
from .model_interface import ModelInterface

class ModelFactory:
    """模型工厂类"""
    
    def __init__(self):
        self._model_registry = {}
    
    def register_model(self, model_type: str, model_class):
        """注册模型类
        
        Args:
            model_type: 模型类型
            model_class: 模型类
        """
        self._model_registry[model_type] = model_class
    
    def create_model(self, model_type: str, model_name: str, **kwargs) -> ModelInterface:
        """创建模型实例
        
        Args:
            model_type: 模型类型
            model_name: 模型名称
            **kwargs: 模型初始化参数
            
        Returns:
            模型实例
        
        Raises:
            ValueError: 如果模型类型未注册
        """
        if model_type not in self._model_registry:
            raise ValueError(f"Model type '{model_type}' is not registered")
        
        model_class = self._model_registry[model_type]
        return model_class(model_name, **kwargs)

# 创建全局模型工厂实例
model_factory = ModelFactory()