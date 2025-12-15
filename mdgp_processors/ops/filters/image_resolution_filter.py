"""
图像分辨率过滤算子
"""

from ..base_operator import Operator

class ImageResolutionFilter(Operator):
    """图像分辨率过滤算子"""
    
    def __init__(self, text_column: str = "text", min_width: int = 0, min_height: int = 0, max_width: int = None, max_height: int = None):
        """初始化图像分辨率过滤器
        
        Args:
            text_column: 文本列名（用于关联图像数据）
            min_width: 最小宽度
            min_height: 最小高度
            max_width: 最大宽度
            max_height: 最大高度
        """
        super().__init__()
        self.text_column = text_column
        self.min_width = min_width
        self.min_height = min_height
        self.max_width = max_width
        self.max_height = max_height
    
    def process(self, dataframe):
        """过滤分辨率不在范围内的图像
        
        Args:
            dataframe: 输入数据框，需包含指定的文本列和'width'、'height'列
        
        Returns:
            过滤后的数据框
        """
        # 首先过滤掉文本列为空的数据
        dataframe = dataframe.filter(dataframe[self.text_column].is_not_null())
        
        # 应用宽度过滤条件
        if self.min_width > 0:
            dataframe = dataframe.filter(dataframe["width"] >= self.min_width)
        
        if self.max_width is not None:
            dataframe = dataframe.filter(dataframe["width"] <= self.max_width)
        
        # 应用高度过滤条件
        if self.min_height > 0:
            dataframe = dataframe.filter(dataframe["height"] >= self.min_height)
        
        if self.max_height is not None:
            dataframe = dataframe.filter(dataframe["height"] <= self.max_height)
        
        return dataframe