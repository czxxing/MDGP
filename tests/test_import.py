#!/usr/bin/env python3
"""
测试MDGP Processors包的导入功能
"""

try:
    from mdgp_processors import (
        DataPipeline,
        # Readers
        CSVReader,
        JSONReader,
        ParquetReader,
        ImageReader,
        AudioReader,
        LanceReader,
        # Filters
        TextLengthFilter,
        ImageResolutionFilter,
        AudioDurationFilter,
        QualityScoreFilter,
        # Evaluators
        TextQualityEvaluator,
        # Dedupers
        TextDeduper,
        # Writers
        CSVWriter,
        LanceWriter,
    )
    
    print("✅ 所有类导入成功！")
    print(f"导入的类数量: {len([name for name in dir() if not name.startswith('_')]) - 1}")  # 减去__file__
    
    # 打印导入的类名
    imported_classes = [name for name in dir() if not name.startswith('_') and name != 'print']
    print("\n导入的类:")
    for cls in imported_classes:
        print(f"  - {cls}")
        
except ImportError as e:
    print(f"❌ 导入失败: {e}")