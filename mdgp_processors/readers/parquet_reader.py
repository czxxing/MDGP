"""
Parquet文件读取算子
"""

import daft
from ..pipeline import Operator

class ParquetReader(Operator):
    """Parquet文件读取算子"""
    
    def __init__(self, file_path: str, **kwargs):
        """初始化Parquet读取器
        
        Args:
            file_path: Parquet文件路径
            **kwargs: 传递给daft.read_parquet的其他参数
        """
        super().__init__()
        self.file_path = file_path
        self.kwargs = kwargs
    
    def process(self, dataframe: daft.DataFrame = None) -> daft.DataFrame:
        """读取Parquet文件并返回Daft DataFrame
        
        Args:
            dataframe: 输入数据框（可选，此处忽略）
        
        Returns:
            读取后的Daft DataFrame
        """
        return daft.read_parquet(self.file_path, **self.kwargs)