"""
文本去重算子
"""

from ..base_operator import Operator

class TextDeduper(Operator):
    """文本去重算子"""
    
    def __init__(self, text_column: str = "text", keep: str = "first"):
        """初始化文本去重器
        
        Args:
            text_column: 文本列名
            keep: 保留策略，'first'、'last'或False
        """
        super().__init__()
        self.text_column = text_column
        self.keep = keep
    
    def process(self, dataframe):
        """去除重复文本
        
        Args:
            dataframe: 输入数据框，需包含指定的文本列
        
        Returns:
            去重后的数据框
        """
        return dataframe.drop_duplicates(self.text_column)