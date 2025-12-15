"""
数据可视化模块
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import Dict, List, Union, Optional
import os

class DataVisualizer:
    """
    数据可视化类，用于生成各种数据分布图
    """
    
    def __init__(self, dataframe: pd.DataFrame):
        """
        初始化数据可视化器
        
        Args:
            dataframe: 要可视化的Pandas数据框
        """
        self.dataframe = dataframe
        # 设置中文字体支持
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        # 设置图表风格
        sns.set_style("whitegrid")
    
    def plot_histogram(self, column: str, bins: int = 30, title: Optional[str] = None, 
                       save_path: Optional[str] = None) -> plt.Figure:
        """
        绘制直方图
        
        Args:
            column: 要绘制的列名
            bins: 直方图的柱数
            title: 图表标题
            save_path: 保存路径，如果为None则不保存
            
        Returns:
            生成的Figure对象
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.histplot(data=self.dataframe, x=column, bins=bins, kde=True, ax=ax)
        
        if title is None:
            title = f"{column} 分布直方图"
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel(column, fontsize=12)
        ax.set_ylabel("频率", fontsize=12)
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def plot_boxplot(self, column: str, title: Optional[str] = None, 
                    save_path: Optional[str] = None) -> plt.Figure:
        """
        绘制箱线图
        
        Args:
            column: 要绘制的列名
            title: 图表标题
            save_path: 保存路径，如果为None则不保存
            
        Returns:
            生成的Figure对象
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.boxplot(data=self.dataframe, y=column, ax=ax)
        
        if title is None:
            title = f"{column} 箱线图"
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_ylabel(column, fontsize=12)
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def plot_scatter(self, x_column: str, y_column: str, hue: Optional[str] = None, 
                    title: Optional[str] = None, save_path: Optional[str] = None) -> plt.Figure:
        """
        绘制散点图
        
        Args:
            x_column: X轴列名
            y_column: Y轴列名
            hue: 分组列名
            title: 图表标题
            save_path: 保存路径，如果为None则不保存
            
        Returns:
            生成的Figure对象
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.scatterplot(data=self.dataframe, x=x_column, y=y_column, hue=hue, ax=ax)
        
        if title is None:
            title = f"{x_column} vs {y_column} 散点图"
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel(x_column, fontsize=12)
        ax.set_ylabel(y_column, fontsize=12)
        
        if hue:
            ax.legend(title=hue)
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def plot_correlation_heatmap(self, numeric_columns: Optional[List[str]] = None, 
                                title: Optional[str] = None, save_path: Optional[str] = None) -> plt.Figure:
        """
        绘制相关系数热力图
        
        Args:
            numeric_columns: 要计算相关系数的数值列列表，如果为None则使用所有数值列
            title: 图表标题
            save_path: 保存路径，如果为None则不保存
            
        Returns:
            生成的Figure对象
        """
        if numeric_columns is None:
            numeric_columns = self.dataframe.select_dtypes(include=[np.number]).columns.tolist()
        
        correlation_matrix = self.dataframe[numeric_columns].corr()
        
        fig, ax = plt.subplots(figsize=(12, 10))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, 
                   square=True, linewidths=0.5, cbar_kws={'shrink': 0.8}, ax=ax)
        
        if title is None:
            title = "相关系数热力图"
        ax.set_title(title, fontsize=14, fontweight='bold')
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def plot_bar_chart(self, column: str, top_n: Optional[int] = None, 
                      title: Optional[str] = None, save_path: Optional[str] = None) -> plt.Figure:
        """
        绘制条形图
        
        Args:
            column: 要绘制的列名
            top_n: 只显示前N个最常见的值
            title: 图表标题
            save_path: 保存路径，如果为None则不保存
            
        Returns:
            生成的Figure对象
        """
        value_counts = self.dataframe[column].value_counts()
        
        if top_n is not None:
            value_counts = value_counts.head(top_n)
        
        fig, ax = plt.subplots(figsize=(10, 6))
        value_counts.plot(kind='bar', ax=ax)
        
        if title is None:
            title = f"{column} 条形图"
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel(column, fontsize=12)
        ax.set_ylabel("计数", fontsize=12)
        
        # 旋转x轴标签以避免重叠
        plt.xticks(rotation=45, ha='right')
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def plot_wordcloud(self, column: str, title: Optional[str] = None, 
                      save_path: Optional[str] = None, **kwargs) -> plt.Figure:
        """
        绘制词云图
        
        Args:
            column: 要绘制的文本列名
            title: 图表标题
            save_path: 保存路径，如果为None则不保存
            **kwargs: 传递给WordCloud的参数
            
        Returns:
            生成的Figure对象
        """
        try:
            from wordcloud import WordCloud
        except ImportError:
            raise ImportError("请安装wordcloud库: pip install wordcloud")
        
        # 合并所有文本
        text = ' '.join(self.dataframe[column].dropna().tolist())
        
        # 创建词云
        wordcloud = WordCloud(width=800, height=400, background_color='white', **kwargs).generate(text)
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        
        if title is None:
            title = f"{column} 词云图"
        ax.set_title(title, fontsize=14, fontweight='bold')
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def plot_distribution_comparison(self, columns: List[str], title: Optional[str] = None, 
                                    save_path: Optional[str] = None) -> plt.Figure:
        """
        绘制多个列的分布比较图
        
        Args:
            columns: 要比较的列名列表
            title: 图表标题
            save_path: 保存路径，如果为None则不保存
            
        Returns:
            生成的Figure对象
        """
        fig, ax = plt.subplots(figsize=(12, 6))
        
        for column in columns:
            sns.kdeplot(data=self.dataframe, x=column, label=column, ax=ax)
        
        if title is None:
            title = "多列分布比较图"
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel("值", fontsize=12)
        ax.set_ylabel("密度", fontsize=12)
        ax.legend()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        return fig
    
    def create_report(self, output_dir: str, numeric_columns: Optional[List[str]] = None, 
                     text_columns: Optional[List[str]] = None) -> None:
        """
        创建完整的数据分析报告
        
        Args:
            output_dir: 报告输出目录
            numeric_columns: 要分析的数值列列表，如果为None则使用所有数值列
            text_columns: 要分析的文本列列表，如果为None则使用所有文本列
        """
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        if numeric_columns is None:
            numeric_columns = self.dataframe.select_dtypes(include=[np.number]).columns.tolist()
        
        if text_columns is None:
            text_columns = self.dataframe.select_dtypes(include=['object']).columns.tolist()
        
        # 生成数值列的图表
        for column in numeric_columns:
            self.plot_histogram(column, save_path=os.path.join(output_dir, f"{column}_histogram.png"))
            self.plot_boxplot(column, save_path=os.path.join(output_dir, f"{column}_boxplot.png"))
        
        # 生成文本列的图表
        for column in text_columns:
            self.plot_bar_chart(column, top_n=20, save_path=os.path.join(output_dir, f"{column}_bar_chart.png"))
        
        # 生成相关系数热力图
        if len(numeric_columns) >= 2:
            self.plot_correlation_heatmap(numeric_columns, 
                                         save_path=os.path.join(output_dir, "correlation_heatmap.png"))
        
        plt.close('all')