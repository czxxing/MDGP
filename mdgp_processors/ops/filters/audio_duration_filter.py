"""
音频时长过滤算子
"""

from ..pipeline import Operator

class AudioDurationFilter(Operator):
    """音频时长过滤算子"""
    
    def __init__(self, text_column: str = "text", min_duration: float = 0.0, max_duration: float = None):
        """初始化音频时长过滤器
        
        Args:
            text_column: 文本列名（用于关联音频数据）
            min_duration: 最小时长（秒）
            max_duration: 最大时长（秒）
        """
        super().__init__()
        self.text_column = text_column
        self.min_duration = min_duration
        self.max_duration = max_duration
    
    def process(self, dataframe):
        """过滤时长不在范围内的音频
        
        Args:
            dataframe: 输入数据框，需包含指定的文本列和'duration'列
        
        Returns:
            过滤后的数据框
        """
        # 应用时长过滤条件
        if self.min_duration > 0.0:
            dataframe = dataframe.filter(dataframe["duration"] >= self.min_duration)
        
        if self.max_duration is not None:
            dataframe = dataframe.filter(dataframe["duration"] <= self.max_duration)
        
        return dataframe