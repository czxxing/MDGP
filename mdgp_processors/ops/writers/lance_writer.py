"""
Lance格式写入算子
"""

from ..base_operator import Operator

class LanceWriter(Operator):
    """Lance格式写入算子"""
    
    def __init__(self, file_path: str, **kwargs):
        """初始化Lance写入器
        
        Args:
            file_path: Lance文件输出路径或目录
            **kwargs: 传递给dataframe.write_lance的其他参数
        """
        super().__init__()
        self.file_path = file_path
        self.kwargs = kwargs
    
    def process(self, dataframe):
        """将数据写入Lance格式
        
        Args:
            dataframe: 输入数据框
        
        Returns:
            处理后的数据框（保持不变）
        """
        dataframe.write_lance(self.file_path, **self.kwargs)
        return dataframe