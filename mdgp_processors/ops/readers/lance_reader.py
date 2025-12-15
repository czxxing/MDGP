"""
Lance格式读取算子
"""

import daft
from ..pipeline import Operator

class LanceReader(Operator):
    """Lance格式读取算子"""
    
    def __init__(self, file_path: str, **kwargs):
        """初始化Lance读取器
        
        Args:
            file_path: Lance文件路径或目录
            **kwargs: 传递给daft.read_lance的其他参数
        """
        super().__init__()
        self.file_path = file_path
        self.kwargs = kwargs
    
    def process(self, dataframe: daft.DataFrame = None) -> daft.DataFrame:
        """读取Lance格式数据并返回Daft DataFrame
        
        Args:
            dataframe: 输入数据框（可选，此处忽略）
        
        Returns:
            读取后的Daft DataFrame
        """
        return daft.read_lance(self.file_path, **self.kwargs)