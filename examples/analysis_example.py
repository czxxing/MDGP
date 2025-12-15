"""
示例脚本：展示如何使用analysis包进行数据分析
"""

import daft
import pandas as pd
import numpy as np
import os
from mdgp_processors import (
    DataAnalyzer,
    DataVisualizer,
    EvaluationAnalyzer,
    TextQualityEvaluator,
    DataPipeline
)

# 创建示例数据
def create_sample_data():
    """创建包含评估结果的示例数据"""
    np.random.seed(42)
    
    # 创建文本数据
    texts = [
        "这是一个高质量的文本样本，内容丰富且结构清晰。",
        "简短文本。",
        "质量一般的文本，没有特别的亮点。",
        "非常好的文本！详细介绍了主题内容。",
        "较差的文本，内容不完整。",
        "这是一个中等质量的文本，有一些有用的信息。",
        "优秀的文本，逻辑严谨，表达流畅。",
        "糟糕的文本，几乎没有实质内容。",
        "普通文本，没有什么特别之处。",
        "完美的文本，各方面都很出色。"
    ] * 20  # 200个样本
    
    # 创建数据框
    data = {
        "text": texts,
        "length": [len(text) for text in texts],
        "category": np.random.choice(["科技", "娱乐", "教育", "新闻"], size=200),
        "eval_text_quality": np.clip(np.random.normal(0.7, 0.15, 200), 0, 1),
        "eval_readability": np.clip(np.random.normal(0.65, 0.12, 200), 0, 1),
        "eval_coherence": np.clip(np.random.normal(0.72, 0.10, 200), 0, 1),
        "eval_relevance": np.clip(np.random.normal(0.68, 0.13, 200), 0, 1)
    }
    
    df = pd.DataFrame(data)
    return daft.from_pandas(df)

# 主函数
def main():
    print("🚀 开始数据分析示例")
    print("=" * 50)
    
    # 1. 创建示例数据
    print("\n📋 1. 创建示例数据...")
    daft_df = create_sample_data()
    pandas_df = daft_df.to_pandas()
    print(f"✅ 数据创建成功，包含 {len(pandas_df)} 行样本")
    print(f"   列名: {list(pandas_df.columns)}")
    
    # 2. 使用DataAnalyzer分析数据分布
    print("\n📊 2. 使用DataAnalyzer分析数据分布...")
    data_analyzer = DataAnalyzer(daft_df)
    
    # 分析单个评估列
    quality_stats = data_analyzer.analyze_column_distribution("eval_text_quality")
    print(f"\n📈 文本质量评估列分析:")
    print(f"   平均值: {quality_stats['mean']:.2f}")
    print(f"   中位数: {quality_stats['median']:.2f}")
    print(f"   最小值: {quality_stats['min']:.2f}")
    print(f"   最大值: {quality_stats['max']:.2f}")
    print(f"   标准差: {quality_stats['std']:.2f}")
    
    # 分析所有评估列
    all_eval_stats = data_analyzer.analyze_evaluation_columns()
    print(f"\n📊 所有评估列分析完成，共 {len(all_eval_stats)} 个评估列")
    
    # 3. 使用DataVisualizer生成图表
    print("\n🎨 3. 使用DataVisualizer生成图表...")
    visualizer = DataVisualizer(pandas_df)
    
    # 创建输出目录
    output_dir = "analysis_results"
    os.makedirs(output_dir, exist_ok=True)
    
    # 生成直方图
    visualizer.plot_histogram("eval_text_quality",
                            title="文本质量评估分布直方图",
                            save_path=os.path.join(output_dir, "text_quality_histogram.png"))
    print(f"✅ 直方图已保存到: {os.path.join(output_dir, 'text_quality_histogram.png')}")
    
    # 生成箱线图
    visualizer.plot_boxplot("eval_readability",
                          title="可读性评估箱线图",
                          save_path=os.path.join(output_dir, "readability_boxplot.png"))
    print(f"✅ 箱线图已保存到: {os.path.join(output_dir, 'readability_boxplot.png')}")
    
    # 生成相关系数热力图
    visualizer.plot_correlation_heatmap(
        title="评估列相关系数热力图",
        save_path=os.path.join(output_dir, "correlation_heatmap.png")
    )
    print(f"✅ 相关系数热力图已保存到: {os.path.join(output_dir, 'correlation_heatmap.png')}")
    
    # 4. 使用EvaluationAnalyzer分析评估结果
    print("\n📈 4. 使用EvaluationAnalyzer分析评估结果...")
    eval_analyzer = EvaluationAnalyzer(daft_df)
    
    # 计算通过率
    pass_rates = eval_analyzer.calculate_all_pass_rates(threshold=0.7)
    print(f"\n📊 各评估列通过率 (阈值: 0.7):")
    for col, stats in pass_rates.items():
        print(f"   {col}: {stats['pass_rate']:.1f}%")
    
    # 5. 生成完整评估报告
    print("\n📋 5. 生成完整评估报告...")
    report_dir = os.path.join(output_dir, "evaluation_report")
    eval_analyzer.generate_evaluation_report(report_dir)
    
    # 6. 数据分析完成
    print("\n🎉 数据分析示例完成！")
    print("=" * 50)
    print(f"📁 所有结果已保存到: {output_dir} 目录")

if __name__ == "__main__":
    main()