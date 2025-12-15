"""
测试脚本：验证analysis包的功能
"""

import daft
import pandas as pd
import numpy as np
import os
import tempfile
from mdgp_processors import (
    DataAnalyzer,
    DataVisualizer,
    EvaluationAnalyzer,
    TextQualityEvaluator,
    DataPipeline
)

# 创建测试数据
def create_test_data():
    # 创建包含文本质量评估结果的测试数据
    np.random.seed(42)
    
    data = {
        "text": [
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
        ] * 10,  # 复制10次以增加样本量
        "original_quality": np.random.normal(0.7, 0.1, 100),
        "length": np.random.randint(10, 200, 100)
    }
    
    # 创建Pandas DataFrame
    df = pd.DataFrame(data)
    
    # 添加一些评估列
    df["eval_text_quality"] = np.clip(df["original_quality"] + np.random.normal(0, 0.05, 100), 0, 1)
    df["eval_readability"] = np.clip(0.6 + np.random.normal(0, 0.1, 100), 0, 1)
    df["eval_coherence"] = np.clip(0.7 + np.random.normal(0, 0.08, 100), 0, 1)
    
    # 转换为Daft DataFrame
    return daft.from_pandas(df)

# 测试DataAnalyzer
def test_data_analyzer():
    print("📊 测试DataAnalyzer...")
    
    # 创建测试数据
    daft_df = create_test_data()
    
    # 创建DataAnalyzer实例
    analyzer = DataAnalyzer(daft_df)
    
    # 分析单个列
    text_stats = analyzer.analyze_column_distribution("text")
    print(f"✅ 文本列分析成功，包含 {len(text_stats)} 个统计指标")
    
    # 分析数值列
    quality_stats = analyzer.analyze_column_distribution("eval_text_quality")
    print(f"✅ 质量评估列分析成功，平均值: {quality_stats['mean']:.2f}")
    
    # 分析所有列
    all_stats = analyzer.analyze_all_columns()
    print(f"✅ 所有列分析成功，共分析 {len(all_stats)} 个列")
    
    # 检测异常值
    outliers = analyzer.detect_outliers("length", method="iqr")
    print(f"✅ 异常值检测成功，找到 {len(outliers)} 个异常值")
    
    # 获取相关系数矩阵
    corr_matrix = analyzer.get_correlation_matrix()
    print(f"✅ 相关系数矩阵计算成功，形状: {corr_matrix.shape}")
    
    print("🎉 DataAnalyzer测试通过！")

# 测试DataVisualizer
def test_data_visualizer():
    print("\n🎨 测试DataVisualizer...")
    
    # 创建测试数据
    daft_df = create_test_data()
    pandas_df = daft_df.to_pandas()
    
    # 创建DataVisualizer实例
    visualizer = DataVisualizer(pandas_df)
    
    # 使用临时目录保存图表
    with tempfile.TemporaryDirectory() as tmpdir:
        # 测试直方图
        fig_hist = visualizer.plot_histogram("eval_text_quality", 
                                           save_path=os.path.join(tmpdir, "histogram.png"))
        print(f"✅ 直方图绘制成功")
        
        # 测试箱线图
        fig_box = visualizer.plot_boxplot("length", 
                                         save_path=os.path.join(tmpdir, "boxplot.png"))
        print(f"✅ 箱线图绘制成功")
        
        # 测试散点图
        fig_scatter = visualizer.plot_scatter("eval_text_quality", "eval_readability", 
                                             save_path=os.path.join(tmpdir, "scatter.png"))
        print(f"✅ 散点图绘制成功")
        
        # 测试相关系数热力图
        fig_heatmap = visualizer.plot_correlation_heatmap(
            save_path=os.path.join(tmpdir, "heatmap.png")
        )
        print(f"✅ 相关系数热力图绘制成功")
        
        # 测试条形图
        # 创建一个分类列用于测试条形图
        pandas_df["category"] = np.random.choice(["A", "B", "C", "D"], size=len(pandas_df))
        visualizer = DataVisualizer(pandas_df)  # 更新visualizer
        fig_bar = visualizer.plot_bar_chart("category", 
                                           save_path=os.path.join(tmpdir, "bar_chart.png"))
        print(f"✅ 条形图绘制成功")
        
        # 测试分布比较图
        fig_compare = visualizer.plot_distribution_comparison(
            ["eval_text_quality", "eval_readability", "eval_coherence"],
            save_path=os.path.join(tmpdir, "distribution_comparison.png")
        )
        print(f"✅ 分布比较图绘制成功")
    
    print("🎉 DataVisualizer测试通过！")

# 测试EvaluationAnalyzer
def test_evaluation_analyzer():
    print("\n📈 测试EvaluationAnalyzer...")
    
    # 创建测试数据
    daft_df = create_test_data()
    
    # 创建EvaluationAnalyzer实例
    eval_analyzer = EvaluationAnalyzer(daft_df)
    
    # 分析评估列
    eval_stats = eval_analyzer.analyze_evaluation_columns()
    print(f"✅ 评估列分析成功，共分析 {len(eval_stats)} 个评估列")
    
    # 计算通过率
    pass_rate = eval_analyzer.calculate_pass_rate("eval_text_quality", threshold=0.7)
    print(f"✅ 通过率计算成功，eval_text_quality 通过率: {pass_rate['pass_rate']:.1f}%")
    
    # 比较所有算子
    pass_rates_df = eval_analyzer.compare_operators(threshold=0.7)
    print(f"✅ 算子比较成功，包含 {len(pass_rates_df)} 个评估列")
    
    # 生成评估报告
    with tempfile.TemporaryDirectory() as tmpdir:
        eval_analyzer.generate_evaluation_report(tmpdir)
        
        # 检查报告文件是否生成
        report_files = os.listdir(tmpdir)
        expected_files = ["pass_rates.csv", "evaluation_stats.csv"]
        for file in expected_files:
            if file in report_files:
                print(f"✅ {file} 生成成功")
            else:
                print(f"❌ {file} 生成失败")
    
    # 分析算子影响
    operator_columns = ["eval_text_quality", "eval_readability", "eval_coherence"]
    impact = eval_analyzer.analyze_operator_impact(operator_columns, "original_quality")
    print(f"✅ 算子影响分析成功，分析了 {len(impact)} 个算子")
    
    print("🎉 EvaluationAnalyzer测试通过！")

# 主测试函数
def main():
    print("🚀 开始测试analysis包...")
    
    try:
        test_data_analyzer()
        test_data_visualizer()
        test_evaluation_analyzer()
        
        print("\n🎊 所有测试通过！analysis包功能正常。")
        return True
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()