"""
Hugging Face模型实现，用于调用Hugging Face模型库
"""

from typing import Any, Dict, List, Optional, Union
from .model_interface import ModelInterface
from .model_factory import model_factory

# 延迟导入以避免安装依赖
_transformers_available = False
_torch_available = False

# 尝试导入必要的库
try:
    from transformers import (
        AutoModelForCausalLM,
        AutoModelForSequenceClassification,
        AutoModelForTokenClassification,
        AutoTokenizer,
        pipeline
    )
    import torch
    _transformers_available = True
    _torch_available = True
except ImportError:
    pass

class HuggingFaceModel(ModelInterface):
    """Hugging Face模型实现"""
    
    def __init__(self, model_name: str, device: Optional[Union[str, int]] = None, **kwargs):
        """初始化Hugging Face模型
        
        Args:
            model_name: 模型名称
            device: 设备名称或ID
            **kwargs: 模型初始化参数
        """
        if not _transformers_available:
            raise ImportError("Hugging Face Transformers is not installed")
            
        super().__init__()
        self.model_name = model_name
        self.device = device
        self.kwargs = kwargs
        
        # 初始化tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, **kwargs)
        
        # 初始化各种任务的pipeline
        self._generate_pipeline = None
        self._embeddings_pipeline = None
        self._classify_pipeline = None
    
    def generate(self, inputs: List[str], **kwargs) -> List[str]:
        """生成文本
        
        Args:
            inputs: 输入文本列表
            **kwargs: 生成参数
            
        Returns:
            生成的文本列表
        """
        if self._generate_pipeline is None:
            self._generate_pipeline = pipeline(
                "text-generation",
                model=self.model_name,
                tokenizer=self.tokenizer,
                device=self.device,
                **self.kwargs
            )
        
        results = self._generate_pipeline(inputs, **kwargs)
        return [result[0]["generated_text"] for result in results]
    
    def embeddings(self, inputs: List[str], **kwargs) -> List[List[float]]:
        """生成文本嵌入
        
        Args:
            inputs: 输入文本列表
            **kwargs: 嵌入生成参数
            
        Returns:
            文本嵌入列表
        """
        # 使用预训练的句子嵌入模型
        if self._embeddings_pipeline is None:
            from sentence_transformers import SentenceTransformer
            self._embeddings_pipeline = SentenceTransformer(self.model_name)
        
        return self._embeddings_pipeline.encode(inputs, **kwargs).tolist()
    
    def classify(self, inputs: List[str], labels: List[str], **kwargs) -> List[Dict[str, Any]]:
        """文本分类
        
        Args:
            inputs: 输入文本列表
            labels: 分类标签列表
            **kwargs: 分类参数
            
        Returns:
            分类结果列表，每个结果包含标签和置信度
        """
        if self._classify_pipeline is None:
            self._classify_pipeline = pipeline(
                "zero-shot-classification",
                model=self.model_name,
                tokenizer=self.tokenizer,
                device=self.device,
                **self.kwargs
            )
        
        results = self._classify_pipeline(inputs, candidate_labels=labels, **kwargs)
        
        # 格式化结果
        formatted_results = []
        for result in results:
            formatted_result = {
                "labels": result["labels"],
                "scores": result["scores"]
            }
            formatted_results.append(formatted_result)
        
        return formatted_results
    
    def close(self):
        """关闭模型资源"""
        # 清理pipeline
        self._generate_pipeline = None
        self._embeddings_pipeline = None
        self._classify_pipeline = None
        
        # 清理tokenizer
        self.tokenizer = None

# 注册Hugging Face模型
model_factory.register_model("huggingface", HuggingFaceModel)