"""
文本质量评估算子
"""

from ..pipeline import Operator

class TextQualityEvaluator(Operator):
    """文本质量评估算子"""
    
    def __init__(self, text_column: str = "text", score_column: str = "text_quality_score"):
        """初始化文本质量评估器
        
        Args:
            text_column: 文本列名
            score_column: 质量分数输出列名
        """
        super().__init__()
        self.text_column = text_column
        self.score_column = score_column
    
    def process(self, dataframe):
        """评估文本质量并添加质量分数列
        
        Args:
            dataframe: 输入数据框，需包含指定的文本列
        
        Returns:
            包含质量分数列的数据框
        """
        # 简单的文本质量评估：基于文本长度和标点符号数量
        def evaluate_quality(text):
            if not text:
                return 0.0
            
            # 计算文本长度得分 (0-0.5)
            length_score = min(len(text) / 1000, 0.5)
            
            # 计算标点符号得分 (0-0.3)
            punctuation_count = sum(1 for c in text if c in ".!?，。！？")
            punctuation_score = min(punctuation_count / 20, 0.3)
            
            # 计算单词数得分 (0-0.2)
            word_count = len(text.split())
            word_score = min(word_count / 100, 0.2)
            
            return length_score + punctuation_score + word_score
        
        # 应用评估函数
        return dataframe.with_column(
            self.score_column,
            dataframe[self.text_column].apply(evaluate_quality)
        )