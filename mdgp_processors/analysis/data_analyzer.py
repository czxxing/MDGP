"""
数据分布分析模块
"""

import daft
import pandas as pd
import numpy as np
from typing import Dict, List, Union, Optional

class DataAnalyzer:
    """
    数据分布分析类，用于分析数据框中各列的分布情况
    """
    
    def __init__(self, dataframe: daft.DataFrame):
        """
        初始化数据分析仪
        
        Args:
            dataframe: 要分析的Daft数据框
        """
        self.dataframe = dataframe
        self.pandas_df = dataframe.to_pandas()
    
    def analyze_column_distribution(self, column: str) -> Dict[str, Union[str, float]]:
        """
        分析指定列的分布情况
        
        Args:
            column: 要分析的列名
            
        Returns:
            包含列分布统计信息的字典
        """
        if column not in self.pandas_df.columns:
            raise ValueError(f"列 {column} 不存在于数据框中")
        
        series = self.pandas_df[column]
        dtype = str(series.dtype)
        
        result = {
            "column": column,
            "dtype": dtype,
            "total_rows": len(series),
            "non_null_count": series.count(),
            "null_count": series.isnull().sum(),
            "null_percentage": (series.isnull().sum() / len(series)) * 100
        }
        
        # 根据数据类型进行不同的统计
        if pd.api.types.is_numeric_dtype(series):
            result.update({
                "min": float(series.min()),
                "max": float(series.max()),
                "mean": float(series.mean()),
                "median": float(series.median()),
                "std": float(series.std()),
                "q25": float(series.quantile(0.25)),
                "q50": float(series.quantile(0.5)),
                "q75": float(series.quantile(0.75))
            })
        elif pd.api.types.is_string_dtype(series):
            result.update({
                "unique_count": series.nunique(),
                "unique_percentage": (series.nunique() / len(series)) * 100,
                "min_length": series.dropna().str.len().min() if series.notnull().any() else 0,
                "max_length": series.dropna().str.len().max() if series.notnull().any() else 0,
                "avg_length": series.dropna().str.len().mean() if series.notnull().any() else 0
            })
        
        return result
    
    def analyze_all_columns(self) -> Dict[str, Dict[str, Union[str, float]]]:
        """
        分析所有列的分布情况
        
        Returns:
            包含所有列分布统计信息的字典
        """
        results = {}
        for column in self.pandas_df.columns:
            results[column] = self.analyze_column_distribution(column)
        return results
    
    def detect_outliers(self, column: str, method: str = "iqr", threshold: float = 1.5) -> pd.DataFrame:
        """
        检测指定列中的异常值
        
        Args:
            column: 要检测的列名
            method: 检测方法，可选 "iqr" (四分位距) 或 "zscore" (Z分数)
            threshold: 异常值阈值
            
        Returns:
            包含异常值的DataFrame
        """
        if column not in self.pandas_df.columns:
            raise ValueError(f"列 {column} 不存在于数据框中")
        
        series = self.pandas_df[column]
        
        if not pd.api.types.is_numeric_dtype(series):
            raise TypeError(f"列 {column} 不是数值类型，无法检测异常值")
        
        if method == "iqr":
            # 使用四分位距方法检测异常值
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - (threshold * iqr)
            upper_bound = q3 + (threshold * iqr)
            outliers = self.pandas_df[(series < lower_bound) | (series > upper_bound)]
        elif method == "zscore":
            # 使用Z分数方法检测异常值
            z_scores = (series - series.mean()) / series.std()
            outliers = self.pandas_df[abs(z_scores) > threshold]
        else:
            raise ValueError(f"不支持的检测方法: {method}，请选择 'iqr' 或 'zscore'")
        
        return outliers
    
    def get_correlation_matrix(self, numeric_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取数值列之间的相关系数矩阵
        
        Args:
            numeric_columns: 要计算相关系数的数值列列表，如果为None则使用所有数值列
            
        Returns:
            相关系数矩阵
        """
        if numeric_columns is None:
            numeric_columns = self.pandas_df.select_dtypes(include=[np.number]).columns.tolist()
        
        return self.pandas_df[numeric_columns].corr()