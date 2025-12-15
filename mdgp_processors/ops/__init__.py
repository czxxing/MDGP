"""
算子模块
"""

# 导入所有算子
from .base_operator import Operator
from .evaluators import *
from .filters import *
from .dedupers import *
from .readers import *
from .writers import *

# 导出所有算子
__all__ = ['Operator']
__all__.extend(evaluators.__all__)
__all__.extend(filters.__all__)
__all__.extend(dedupers.__all__)
__all__.extend(readers.__all__)
__all__.extend(writers.__all__)