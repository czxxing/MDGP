"""
图像文件读取算子
"""

import daft
from ..pipeline import Operator

class ImageReader(Operator):
    """图像文件读取算子"""
    
    def __init__(self, file_path: str, **kwargs):
        """初始化图像读取器
        
        Args:
            file_path: 图像文件路径或目录
            **kwargs: 传递给daft.read_images的其他参数
        """
        super().__init__()
        self.file_path = file_path
        self.kwargs = kwargs
    
    def process(self, dataframe: daft.DataFrame = None) -> daft.DataFrame:
        """读取图像文件并返回Daft DataFrame
        
        Args:
            dataframe: 输入数据框（可选，此处忽略）
        
        Returns:
            读取后的Daft DataFrame，包含图像路径和内容
        """
        return daft.read_images(self.file_path, **self.kwargs)