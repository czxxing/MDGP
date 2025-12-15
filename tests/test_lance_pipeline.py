"""
测试Lance格式的读取和写入功能
"""

from mdgp_processors import DataPipeline
from mdgp_processors.readers import LanceReader
from mdgp_processors.filters import TextLengthFilter
from mdgp_processors.dedupers import TextDeduper
from mdgp_processors.writers import LanceWriter

def test_lance_pipeline():
    """测试Lance格式的数据处理管道"""
    print("初始化Lance数据处理管道...")
    
    # 创建管道实例
    pipeline = DataPipeline()
    
    # 构建处理流程
    pipeline.add_operator(LanceReader("db/multimodal_data.lance"))  # 读取Lance格式数据
    pipeline.add_operator(TextLengthFilter(min_length=10))  # 过滤短文本
    pipeline.add_operator(TextDeduper(text_column="text"))  # 文本去重
    pipeline.add_operator(LanceWriter("output/processed_data.lance"))  # 写入Lance格式
    
    print(f"管道构建完成: {pipeline}")
    print("运行数据处理管道...")
    
    # 运行管道
    result = pipeline.run()
    
    print("数据处理完成!")
    print(f"处理结果行数: {result.count().collect()[0][0]}")

if __name__ == "__main__":
    test_lance_pipeline()