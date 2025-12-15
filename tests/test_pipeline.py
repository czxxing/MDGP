"""
测试MDGP Processors数据处理管道
"""

from mdgp_processors import DataPipeline
from mdgp_processors.readers import CSVReader
from mdgp_processors.filters import TextLengthFilter, QualityScoreFilter
from mdgp_processors.dedupers import TextDeduper
from mdgp_processors.writers import CSVWriter

def test_pipeline():
    """测试数据处理管道"""
    print("初始化数据处理管道...")
    
    # 创建管道实例
    pipeline = DataPipeline()
    
    # 构建处理流程
    pipeline.add_operator(CSVReader("data/texts/sample.csv"))  # 读取CSV文件
    pipeline.add_operator(TextLengthFilter(min_length=10))  # 过滤短文本
    pipeline.add_operator(TextDeduper(text_column="content"))  # 文本去重
    pipeline.add_operator(QualityScoreFilter(score_column="quality"))  # 质量过滤
    pipeline.add_operator(CSVWriter("output/processed_data.csv"))  # 导出结果
    
    print(f"管道构建完成: {pipeline}")
    print("运行数据处理管道...")
    
    # 运行管道
    result = pipeline.run()
    
    print("数据处理完成!")
    print(f"处理结果行数: {result.count().collect()[0][0]}")

if __name__ == "__main__":
    test_pipeline()