"""
数据处理管道 - 用于连接各个算子
"""

import daft
from typing import List, TypeVar, Generic

T = TypeVar('T')

class Operator(Generic[T]):
    """算子基类，定义统一接口"""
    
    def __init__(self):
        self.name = self.__class__.__name__
    
    def process(self, dataframe: daft.DataFrame) -> daft.DataFrame:
        """处理数据框的方法，子类必须实现"""
        raise NotImplementedError("子类必须实现process方法")

class DataPipeline:
    """数据处理管道，用于连接多个算子"""
    
    def __init__(self):
        self.operators: List[Operator] = []
        self.dataframe: daft.DataFrame = None
    
    def add_operator(self, operator: Operator) -> 'DataPipeline':
        """添加算子到管道"""
        self.operators.append(operator)
        return self
    
    def set_input(self, dataframe: daft.DataFrame) -> 'DataPipeline':
        """设置输入数据框"""
        self.dataframe = dataframe
        return self
    
    def run(self) -> daft.DataFrame:
        """运行管道，依次执行所有算子"""
        if self.dataframe is None:
            raise ValueError("请先设置输入数据框")
        
        result = self.dataframe
        for operator in self.operators:
            result = operator.process(result)
        
        return result
    
    def __str__(self) -> str:
        """返回管道中算子的名称列表"""
        operator_names = [op.name for op in self.operators]
        return f"DataPipeline: {' -> '.join(operator_names)}"