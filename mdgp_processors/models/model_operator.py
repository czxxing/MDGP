"""
模型调用算子，用于在数据处理流程中调用模型
"""

from typing import Any, Dict, List, Optional, Union
from ..pipeline import Operator
from .model_factory import model_factory

class ModelOperator(Operator):
    """模型调用算子，用于在数据处理流程中调用模型"""
    
    def __init__(self,
                 task: str,
                 model_type: str,
                 model_name: str,
                 text_column: str = "text",
                 output_column: Optional[str] = None,
                 model_params: Optional[Dict[str, Any]] = None,
                 task_params: Optional[Dict[str, Any]] = None):
        """初始化模型调用算子
        
        Args:
            task: 任务类型，支持"generate"、"embeddings"、"classify"
            model_type: 模型类型，支持"local"、"huggingface"、"openai"
            model_name: 模型名称
            text_column: 文本列名
            output_column: 输出列名，如果为None则自动生成
            model_params: 模型初始化参数
            task_params: 任务执行参数
        """
        super().__init__()
        
        # 验证任务类型
        if task not in ["generate", "embeddings", "classify"]:
            raise ValueError(f"Unsupported task type: {task}")
        
        self.task = task
        self.model_type = model_type
        self.model_name = model_name
        self.text_column = text_column
        
        # 设置默认输出列名
        if output_column is None:
            self.output_column = f"{task}_result"
        else:
            self.output_column = output_column
        
        self.model_params = model_params or {}
        self.task_params = task_params or {}
        
        # 创建模型实例
        self.model = model_factory.create_model(
            model_type=model_type,
            model_name=model_name,
            **self.model_params
        )
    
    def process(self, dataframe):
        """处理数据框，调用模型执行指定任务
        
        Args:
            dataframe: 输入数据框，需包含指定的文本列
        
        Returns:
            包含模型执行结果的数据框
        """
        # 获取文本数据
        texts = dataframe.to_pydict()[self.text_column]
        
        # 调用模型执行任务
        if self.task == "generate":
            results = self.model.generate(texts, **self.task_params)
        elif self.task == "embeddings":
            results = self.model.embeddings(texts, **self.task_params)
        elif self.task == "classify":
            # 确保分类任务有标签参数
            if "labels" not in self.task_params:
                raise ValueError("'labels' parameter is required for classify task")
            results = self.model.classify(texts, **self.task_params)
        else:
            raise ValueError(f"Unsupported task type: {self.task}")
        
        # 将结果添加到数据框
        dataframe = dataframe.with_column(self.output_column, results)
        
        return dataframe
    
    def close(self):
        """关闭模型资源"""
        self.model.close()