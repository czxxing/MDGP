"""
测试脚本：验证所有模块是否能正常导入
"""

# 测试导入所有算子和组件
from mdgp_processors import (
    # 核心组件
    Operator, DataPipeline,
    
    # Readers
    CSVReader, JSONReader, ParquetReader, ImageReader, AudioReader, LanceReader,
    
    # Writers
    CSVWriter, LanceWriter,
    
    # Filters
    TextLengthFilter, QualityScoreFilter, ImageResolutionFilter, AudioDurationFilter,
    
    # Evaluators
    TextQualityEvaluator,
    
    # Dedupers
    TextDeduper,
    
    # Models
    ModelOperator, ModelInterface, ModelFactory, model_factory, LocalModel, HuggingFaceModel, OpenAIModel
)

print("✅ 所有模块导入成功！")
print(f"\n已导入的组件数量: {len([item for item in dir() if not item.startswith('_')])}")

# 测试创建各个组件的实例
components_to_test = [
    ("DataPipeline", DataPipeline),
    ("TextLengthFilter", TextLengthFilter),
    ("QualityScoreFilter", QualityScoreFilter),
    ("ImageResolutionFilter", ImageResolutionFilter),
    ("AudioDurationFilter", AudioDurationFilter),
    ("TextQualityEvaluator", TextQualityEvaluator),
    ("TextDeduper", TextDeduper),
    ("ModelOperator", ModelOperator),
]

print("\n🔧 测试组件实例化:")
for name, component in components_to_test:
    try:
        instance = component()
        print(f"   ✅ {name}: 实例化成功")
    except Exception as e:
        print(f"   ❌ {name}: 实例化失败 - {e}")

print("\n🎉 所有测试完成！")