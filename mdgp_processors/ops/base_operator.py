"""
算子基类定义
"""

from typing import TypeVar, Generic

T = TypeVar('T')

class Operator(Generic[T]):
    """算子基类，定义统一接口"""
    
    def __init__(self):
        self.name = self.__class__.__name__
    
    def process(self, dataframe: T) -> T:
        """处理数据框的方法，子类必须实现"""
        raise NotImplementedError("子类必须实现process方法")