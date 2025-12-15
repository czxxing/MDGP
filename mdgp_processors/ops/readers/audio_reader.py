"""
音频文件读取算子
"""

import daft
from ..base_operator import Operator

class AudioReader(Operator):
    """音频文件读取算子"""
    
    def __init__(self, file_path: str, **kwargs):
        """初始化音频读取器
        
        Args:
            file_path: 音频文件路径或目录
            **kwargs: 传递给daft.read_audio的其他参数
        """
        super().__init__()
        self.file_path = file_path
        self.kwargs = kwargs
    
    def process(self, dataframe: daft.DataFrame = None) -> daft.DataFrame:
        """读取音频文件并返回Daft DataFrame
        
        Args:
            dataframe: 输入数据框（可选，此处忽略）
        
        Returns:
            读取后的Daft DataFrame，包含音频路径和内容
        """
        return daft.read_audio(self.file_path, **self.kwargs)