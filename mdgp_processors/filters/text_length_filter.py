"""
文本长度过滤算子
"""

from ..pipeline import Operator

class TextLengthFilter(Operator):
    """文本长度过滤算子"""
    
    def __init__(self, text_column: str = "text", min_length: int = 0, max_length: int = None):
        """初始化文本长度过滤器
        
        Args:
            text_column: 文本列名
            min_length: 最小文本长度
            max_length: 最大文本长度
        """
        super().__init__()
        self.text_column = text_column
        self.min_length = min_length
        self.max_length = max_length
    
    def process(self, dataframe):
        """过滤文本长度不在范围内的数据
        
        Args:
            dataframe: 输入数据框，需包含指定的文本列
        
        Returns:
            过滤后的数据框
        """
        # 计算文本长度
        dataframe = dataframe.with_column("text_length", dataframe[self.text_column].str.len())
        
        # 应用过滤条件
        if self.min_length > 0:
            dataframe = dataframe.filter(dataframe["text_length"] >= self.min_length)
        
        if self.max_length is not None:
            dataframe = dataframe.filter(dataframe["text_length"] <= self.max_length)
        
        # 删除临时列
        dataframe = dataframe.drop("text_length")
        
        return dataframe