"""
CSV文件写入算子
"""

from ..base_operator import Operator

class CSVWriter(Operator):
    """CSV文件写入算子"""
    
    def __init__(self, file_path: str, **kwargs):
        """初始化CSV写入器
        
        Args:
            file_path: CSV文件输出路径
            **kwargs: 传递给daft.write_csv的其他参数
        """
        super().__init__()
        self.file_path = file_path
        self.kwargs = kwargs
    
    def process(self, dataframe):
        """将数据写入CSV文件
        
        Args:
            dataframe: 输入数据框
        
        Returns:
            处理后的数据框（保持不变）
        """
        dataframe.write_csv(self.file_path, **self.kwargs)
        return dataframe