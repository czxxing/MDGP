"""
评估结果分析模块
"""

import daft
import pandas as pd
import numpy as np
from typing import Dict, List, Union, Optional
from .data_analyzer import DataAnalyzer
from .visualizer import DataVisualizer

class EvaluationAnalyzer:
    """
    评估结果分析类，用于分析算子的评估结果
    """
    
    def __init__(self, dataframe: daft.DataFrame):
        """
        初始化评估结果分析仪
        
        Args:
            dataframe: 包含评估结果的Daft数据框
        """
        self.dataframe = dataframe
        self.pandas_df = dataframe.to_pandas()
        self.data_analyzer = DataAnalyzer(dataframe)
        self.visualizer = DataVisualizer(self.pandas_df)
    
    def analyze_evaluation_columns(self, prefix: str = "eval_") -> Dict[str, Dict[str, Union[str, float]]]:
        """
        分析所有评估列（默认以eval_开头）
        
        Args:
            prefix: 评估列的前缀
            
        Returns:
            包含所有评估列分析结果的字典
        """
        evaluation_columns = [col for col in self.pandas_df.columns if col.startswith(prefix)]
        
        if not evaluation_columns:
            raise ValueError(f"没有找到以 '{prefix}' 开头的评估列")
        
        results = {}
        for column in evaluation_columns:
            results[column] = self.data_analyzer.analyze_column_distribution(column)
        
        return results
    
    def calculate_pass_rate(self, column: str, threshold: float = 0.5, 
                           operator_name: Optional[str] = None) -> Dict[str, float]:
        """
        计算评估列的通过率
        
        Args:
            column: 评估列名
            threshold: 通过阈值
            operator_name: 算子名称（用于结果标识）
            
        Returns:
            包含通过率信息的字典
        """
        if column not in self.pandas_df.columns:
            raise ValueError(f"列 {column} 不存在于数据框中")
        
        total = len(self.pandas_df)
        passed = len(self.pandas_df[self.pandas_df[column] >= threshold])
        pass_rate = (passed / total) * 100
        
        result = {
            "operator": operator_name or column,
            "total_samples": total,
            "passed_samples": passed,
            "pass_rate": pass_rate
        }
        
        return result
    
    def calculate_all_pass_rates(self, prefix: str = "eval_", 
                                threshold: float = 0.5) -> Dict[str, Dict[str, float]]:
        """
        计算所有评估列的通过率
        
        Args:
            prefix: 评估列的前缀
            threshold: 通过阈值
            
        Returns:
            包含所有评估列通过率信息的字典
        """
        evaluation_columns = [col for col in self.pandas_df.columns if col.startswith(prefix)]
        
        if not evaluation_columns:
            raise ValueError(f"没有找到以 '{prefix}' 开头的评估列")
        
        results = {}
        for column in evaluation_columns:
            # 从列名中提取算子名称
            operator_name = column[len(prefix):]
            results[column] = self.calculate_pass_rate(column, threshold, operator_name)
        
        return results
    
    def compare_operators(self, prefix: str = "eval_", 
                         threshold: float = 0.5) -> pd.DataFrame:
        """
        比较不同算子的评估结果
        
        Args:
            prefix: 评估列的前缀
            threshold: 通过阈值
            
        Returns:
            包含算子比较结果的DataFrame
        """
        pass_rates = self.calculate_all_pass_rates(prefix, threshold)
        
        # 转换为DataFrame
        df = pd.DataFrame.from_dict(pass_rates, orient='index')
        df = df.sort_values(by='pass_rate', ascending=False)
        
        return df
    
    def generate_evaluation_report(self, output_dir: str, prefix: str = "eval_", 
                                  threshold: float = 0.5) -> None:
        """
        生成完整的评估报告
        
        Args:
            output_dir: 报告输出目录
            prefix: 评估列的前缀
            threshold: 通过阈值
        """
        import os
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 分析评估列
        evaluation_columns = [col for col in self.pandas_df.columns if col.startswith(prefix)]
        
        if not evaluation_columns:
            raise ValueError(f"没有找到以 '{prefix}' 开头的评估列")
        
        # 生成每个评估列的图表
        for column in evaluation_columns:
            # 直方图
            self.visualizer.plot_histogram(
                column, 
                save_path=os.path.join(output_dir, f"{column}_histogram.png")
            )
            
            # 箱线图
            self.visualizer.plot_boxplot(
                column, 
                save_path=os.path.join(output_dir, f"{column}_boxplot.png")
            )
        
        # 生成评估列分布比较图
        if len(evaluation_columns) > 1:
            self.visualizer.plot_distribution_comparison(
                evaluation_columns, 
                title="评估列分布比较",
                save_path=os.path.join(output_dir, "evaluation_distribution_comparison.png")
            )
        
        # 生成相关系数热力图（如果有多个评估列）
        if len(evaluation_columns) > 1:
            self.visualizer.plot_correlation_heatmap(
                evaluation_columns,
                title="评估列相关系数热力图",
                save_path=os.path.join(output_dir, "evaluation_correlation_heatmap.png")
            )
        
        # 计算并保存通过率
        pass_rates_df = self.compare_operators(prefix, threshold)
        pass_rates_df.to_csv(os.path.join(output_dir, "pass_rates.csv"))
        
        # 保存所有评估列的详细统计信息
        stats = self.analyze_evaluation_columns(prefix)
        stats_df = pd.DataFrame.from_dict(stats, orient='index')
        stats_df.to_csv(os.path.join(output_dir, "evaluation_stats.csv"))
        
        print(f"✅ 评估报告已生成到 {output_dir} 目录")
        print(f"📊 报告包含:")
        print(f"   - {len(evaluation_columns)} 个评估列的直方图和箱线图")
        print(f"   - 评估列分布比较图")
        print(f"   - 评估列相关系数热力图")
        print(f"   - 通过率统计 (pass_rates.csv)")
        print(f"   - 详细统计信息 (evaluation_stats.csv)")
    
    def analyze_operator_impact(self, operator_columns: List[str], 
                               base_column: str) -> Dict[str, Dict[str, float]]:
        """
        分析不同算子对结果的影响
        
        Args:
            operator_columns: 算子评估列列表
            base_column: 基准列（如原始质量分）
            
        Returns:
            包含算子影响分析结果的字典
        """
        if base_column not in self.pandas_df.columns:
            raise ValueError(f"基准列 {base_column} 不存在于数据框中")
        
        results = {}
        
        for column in operator_columns:
            if column not in self.pandas_df.columns:
                continue
            
            # 计算与基准列的差异
            diff = self.pandas_df[column] - self.pandas_df[base_column]
            
            # 计算统计信息
            impact_stats = {
                "mean_improvement": diff.mean(),
                "median_improvement": diff.median(),
                "std_improvement": diff.std(),
                "positive_count": (diff > 0).sum(),
                "negative_count": (diff < 0).sum(),
                "no_change_count": (diff == 0).sum(),
                "improvement_rate": (diff > 0).mean() * 100,
                "deterioration_rate": (diff < 0).mean() * 100
            }
            
            results[column] = impact_stats
        
        return results