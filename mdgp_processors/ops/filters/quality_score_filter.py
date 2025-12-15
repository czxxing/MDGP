"""
质量分数过滤算子
"""

from ..pipeline import Operator

class QualityScoreFilter(Operator):
    """质量分数过滤算子"""
    
    def __init__(self, text_column: str = "text", score_column: str = "quality_score", min_score: float = 0.0):
        """初始化质量分数过滤器
        
        Args:
            text_column: 文本列名（用于关联质量分数）
            score_column: 质量分数列名
            min_score: 最低质量分数
        """
        super().__init__()
        self.text_column = text_column
        self.score_column = score_column
        self.min_score = min_score
    
    def process(self, dataframe):
        """过滤质量分数低于阈值的数据
        
        Args:
            dataframe: 输入数据框，需包含指定的文本列和质量分数列
        
        Returns:
            过滤后的数据框
        """

        return dataframe.filter(dataframe[self.score_column] >= self.min_score)