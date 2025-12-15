"""
MDGP Processors - 基于Daft的多模态数据处理包
"""

from .pipeline import DataPipeline
from .readers import *
from .filters import *
from .evaluators import *
from .dedupers import *
from .writers import *

__version__ = "0.1.0"
__all__ = [
    "DataPipeline",
    # Readers
    "CSVReader",
    "JSONReader",
    "ParquetReader",
    "ImageReader",
    "AudioReader",
    "LanceReader",
    # Filters
    "TextLengthFilter",
    "ImageResolutionFilter",
    "AudioDurationFilter",
    "QualityScoreFilter",
    # Evaluators
    "TextQualityEvaluator",
    # Dedupers
    "TextDeduper",
    # Writers
    "CSVWriter",
    "LanceWriter",
]