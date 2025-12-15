"""
JSON文件读取算子
"""

import daft
from ..base_operator import Operator

class JSONReader(Operator):
    """JSON文件读取算子"""
    
    def __init__(self, file_path: str, **kwargs):
        """初始化JSON读取器
        
        Args:
            file_path: JSON文件路径
            **kwargs: 传递给daft.read_json的其他参数
        """
        super().__init__()
        self.file_path = file_path
        self.kwargs = kwargs
    
    def process(self, dataframe: daft.DataFrame = None) -> daft.DataFrame:
        """读取JSON文件并返回Daft DataFrame
        
        Args:
            dataframe: 输入数据框（可选，此处忽略）
        
        Returns:
            读取后的Daft DataFrame
        """
        return daft.read_json(self.file_path, **self.kwargs)