"""
数据处理页面模块 - 使用mdgp_processors进行数据处理工作流构建
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, List, Optional
import logging
import matplotlib.pyplot as plt
from datetime import datetime
import daft
import json
import uuid
import base64
from io import BytesIO

# 导入mdgp_processors
from mdgp_processors import Operator, DataPipeline
from mdgp_processors.ops import (
    # Readers
    CSVReader, LanceReader, JSONReader, ParquetReader,
    ImageReader, AudioReader,
    # Writers
    CSVWriter, LanceWriter,
    # Filters
    TextLengthFilter, ImageResolutionFilter, AudioDurationFilter,
    QualityScoreFilter,
    # Dedupers
    TextDeduper,
    # Evaluators
    TextQualityEvaluator
)

# 设置matplotlib中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

class DataProcessingPage:
    """数据处理页面类 - 使用mdgp_processors构建工作流"""
    
    def __init__(self, lance_manager):
        self.lance_manager = lance_manager
        self.logger = self._setup_logging()
        
        # 初始化会话状态
        if 'current_dataframe' not in st.session_state:
            st.session_state.current_dataframe = None
        if 'workflow_operators' not in st.session_state:
            st.session_state.workflow_operators = []
        if 'workflow_results' not in st.session_state:
            st.session_state.workflow_results = None
        if 'processing_logs' not in st.session_state:
            st.session_state.processing_logs = []
        if 'analysis_results' not in st.session_state:
            st.session_state.analysis_results = {}
    
    def _setup_logging(self):
        """设置日志记录"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def get_title(self):
        """获取页面标题"""
        return "数据处理"
    
    def get_description(self):
        """获取页面描述"""
        return "使用mdgp_processors构建数据处理工作流"
    
    def display(self):
        """显示数据处理内容"""
        st.header("📊 数据处理工作流构建")
        
        # 创建页面布局
        self._setup_page_layout()
        
    def _setup_page_layout(self):
        """设置页面布局"""
        # 使用标签页组织内容
        tab1, tab2, tab3 = st.tabs(["工作流构建", "数据加载", "结果展示"])
        
        with tab1:
            self._display_workflow_builder()
        
        with tab2:
            self._display_data_loading_section()
        
        with tab3:
            self._display_results_section()
    
    def _display_data_loading_section(self):
        """显示数据加载区域"""
        st.subheader("📥 数据加载")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 从数据库加载数据", use_container_width=True):
                with st.spinner("正在从数据库加载数据..."):
                    df = self.lance_manager.load_from_lance()
                    if df is not None and not df.empty:
                        st.session_state.current_dataframe = df
                        st.success(f"✅ 成功加载 {len(df)} 条记录")
                        self._add_log("数据加载", f"成功加载 {len(df)} 条记录")
                    else:
                        st.error("❌ 数据库中没有数据，请先在数据目录页面导入数据")
        
        with col2:
            if st.session_state.current_dataframe is not None:
                st.metric("当前数据量", len(st.session_state.current_dataframe))
            else:
                st.info("📊 等待数据加载")
        
        with col3:
            if st.session_state.current_dataframe is not None:
                if st.button("🗑️ 清除数据", use_container_width=True):
                    st.session_state.current_dataframe = None
                    st.session_state.workflow_results = None
                    st.session_state.processing_logs = []
                    st.session_state.analysis_results = {}
                    st.rerun()
        
        # 数据预览
        if st.session_state.current_dataframe is not None:
            self._display_data_preview()
    
    def _display_data_preview(self):
        """显示数据预览"""
        st.subheader("👀 数据预览")
        
        df = st.session_state.current_dataframe
        
        # 显示基本信息
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("记录数", len(df))
        with col2:
            st.metric("列数", len(df.columns))
        with col3:
            st.metric("数据类型", f"{len(df.select_dtypes(include=['object']).columns)}文本列")
        
        # 显示前几行数据
        with st.expander("查看数据详情"):
            st.dataframe(df.head(10), use_container_width=True)
            
            # 显示列信息
            st.write("**列信息:**")
            col_info = pd.DataFrame({
                '列名': df.columns,
                '数据类型': [str(dtype) for dtype in df.dtypes.values],
                '非空值数': df.count().values,
                '缺失值数': df.isnull().sum().values
            })
            st.dataframe(col_info, use_container_width=True)
    
    def _display_workflow_builder(self):
        """显示工作流构建区域"""
        st.subheader("🔧 工作流构建")
        
        # 算子库和工作流区域
        col1, col2 = st.columns([1, 3], gap="medium")
        
        with col1:
            st.subheader("🧩 算子库")
            self._display_operator_library()
        
        with col2:
            st.subheader("📋 工作流")
            self._display_workflow_canvas()
    
    def _display_operator_library(self):
        """显示算子库"""
        # 算子分类
        operator_categories = {
            "读取器": [CSVReader, LanceReader, JSONReader, ParquetReader, ImageReader, AudioReader],
            "过滤器": [TextLengthFilter, ImageResolutionFilter, AudioDurationFilter, QualityScoreFilter],
            "评估器": [TextQualityEvaluator],
            "去重器": [TextDeduper],
            "写入器": [CSVWriter, LanceWriter]
        }
        
        for category, operators in operator_categories.items():
            with st.expander(f"{category}"):
                for operator_class in operators:
                    if st.button(
                        f"➕ {operator_class.__name__}",
                        use_container_width=True,
                        key=f"add_{operator_class.__name__}"
                    ):
                        self._add_operator_to_workflow(operator_class)
    
    def _add_operator_to_workflow(self, operator_class):
        """添加算子到工作流"""
        # 创建算子实例
        operator_id = str(uuid.uuid4())
        operator = operator_class()
        
        # 保存算子信息
        operator_info = {
            "id": operator_id,
            "class_name": operator_class.__name__,
            "instance": operator,
            "params": self._get_operator_params(operator_class),
            "position": {"x": 100, "y": 100}
        }
        
        # 添加到工作流
        st.session_state.workflow_operators.append(operator_info)
        
        self._add_log("工作流构建", f"添加算子: {operator_class.__name__}")
    
    def _get_operator_params(self, operator_class):
        """获取算子参数信息"""
        # 这里可以通过反射获取算子的参数信息
        # 简单实现，根据不同算子返回默认参数
        params = {}
        
        if operator_class == TextLengthFilter:
            params = {
                "text_column": "text",
                "min_length": 0,
                "max_length": None
            }
        elif operator_class == TextQualityEvaluator:
            params = {
                "text_column": "text",
                "score_column": "text_quality_score"
            }
        elif operator_class == CSVReader:
            params = {
                "file_path": "",
                "delimiter": ","
            }
        elif operator_class == CSVWriter:
            params = {
                "file_path": "",
                "delimiter": ","
            }
        elif operator_class == QualityScoreFilter:
            params = {
                "score_column": "text_quality_score",
                "threshold": 0.5
            }
        
        return params
    
    def _display_workflow_canvas(self):
        """显示工作流画布"""
        # 工作流画布
        workflow_container = st.container(height=500)
        
        with workflow_container:
            # 显示工作流中的算子
            if st.session_state.workflow_operators:
                for i, operator_info in enumerate(st.session_state.workflow_operators):
                    self._display_operator_card(i, operator_info)
                
                # 添加运行按钮
                if st.button("🚀 运行工作流", use_container_width=True):
                    self._run_workflow()
                
                # 添加清除按钮
                if st.button("🗑️ 清除工作流", use_container_width=True):
                    st.session_state.workflow_operators = []
                    st.rerun()
            else:
                st.info("📋 从左侧算子库拖拽算子到此处构建工作流")
    
    def _display_operator_card(self, index: int, operator_info: Dict[str, Any]):
        """显示算子卡片"""
        operator = operator_info["instance"]
        params = operator_info["params"]
        
        with st.expander(f"{index+1}. {operator.name}", expanded=True):
            # 显示算子参数配置
            self._display_operator_params(operator, params)
            
            # 添加删除按钮
            if st.button(f"❌ 删除", key=f"delete_{operator_info['id']}"):
                st.session_state.workflow_operators.pop(index)
                st.rerun()
    
    def _display_operator_params(self, operator: Operator, params: Dict[str, Any]):
        """显示算子参数配置"""
        # 根据算子类型显示不同的参数配置
        if isinstance(operator, TextLengthFilter):
            params["text_column"] = st.text_input("文本列名", value=params["text_column"])
            params["min_length"] = st.number_input("最小长度", min_value=0, value=params["min_length"])
            params["max_length"] = st.number_input("最大长度", min_value=0, value=params["max_length"] or 1000, step=1)
        
        elif isinstance(operator, TextQualityEvaluator):
            params["text_column"] = st.text_input("文本列名", value=params["text_column"])
            params["score_column"] = st.text_input("分数列名", value=params["score_column"])
        
        elif isinstance(operator, QualityScoreFilter):
            params["score_column"] = st.text_input("分数列名", value=params["score_column"])
            params["threshold"] = st.slider("质量阈值", min_value=0.0, max_value=1.0, value=params["threshold"])
        
        elif isinstance(operator, CSVReader):
            params["file_path"] = st.text_input("文件路径", value=params["file_path"])
            params["delimiter"] = st.text_input("分隔符", value=params["delimiter"])
        
        elif isinstance(operator, CSVWriter):
            params["file_path"] = st.text_input("文件路径", value=params["file_path"])
            params["delimiter"] = st.text_input("分隔符", value=params["delimiter"])
    
    def _run_workflow(self):
        """运行工作流"""
        if not st.session_state.workflow_operators:
            st.error("❌ 工作流为空，请添加算子")
            return
        
        if st.session_state.current_dataframe is None:
            st.error("❌ 没有加载数据，请先加载数据")
            return
        
        with st.spinner("正在运行工作流..."):
            try:
                # 转换数据格式
                df = st.session_state.current_dataframe
                
                # 初始化管道
                pipeline = DataPipeline()
                pipeline.set_input(df)
                
                # 添加算子到管道
                for operator_info in st.session_state.workflow_operators:
                    operator = operator_info["instance"]
                    params = operator_info["params"]
                    
                    # 更新算子参数
                    self._update_operator_params(operator, params)
                    
                    pipeline.add_operator(operator)
                
                # 运行管道
                result_df = pipeline.run()
                
                # 保存结果
                st.session_state.workflow_results = result_df
                st.success(f"✅ 工作流运行完成！结果包含 {len(result_df)} 条记录")
                
                self._add_log("工作流运行", f"工作流运行完成，结果包含 {len(result_df)} 条记录")
                
                # 分析结果
                self._analyze_workflow_results(result_df)
                
            except Exception as e:
                st.error(f"❌ 工作流运行失败: {str(e)}")
                self._add_log("工作流运行", f"运行失败: {str(e)}", "ERROR")
    
    def _update_operator_params(self, operator: Operator, params: Dict[str, Any]):
        """更新算子参数"""
        for param_name, param_value in params.items():
            if hasattr(operator, param_name):
                setattr(operator, param_name, param_value)
    
    def _analyze_workflow_results(self, df: daft.DataFrame):
        """分析工作流结果"""
        # 分析结果
        results = {}
        
        # 检查是否有质量分数列
        if "text_quality_score" in df.columns:
            # 计算质量分数统计
            score_stats = self._calculate_score_statistics(df)
            results["text_quality_score"] = score_stats
        
        # 检查是否有文本长度列
        if "text_length" in df.columns:
            # 计算文本长度统计
            length_stats = self._calculate_length_statistics(df)
            results["text_length"] = length_stats
        
        st.session_state.analysis_results = results
    
    def _calculate_score_statistics(self, df: daft.DataFrame):
        """计算质量分数统计"""
        # 将daft DataFrame转换为pandas DataFrame
        pd_df = df.to_pandas()
        
        scores = pd_df["text_quality_score"]
        
        return {
            "mean": scores.mean(),
            "median": scores.median(),
            "std": scores.std(),
            "min": scores.min(),
            "max": scores.max(),
            "count": len(scores),
            "pass_count": (scores >= 0.5).sum(),
            "pass_rate": (scores >= 0.5).sum() / len(scores)
        }
    
    def _calculate_length_statistics(self, df: daft.DataFrame):
        """计算文本长度统计"""
        # 将daft DataFrame转换为pandas DataFrame
        pd_df = df.to_pandas()
        
        lengths = pd_df["text_length"]
        
        return {
            "mean": lengths.mean(),
            "median": lengths.median(),
            "std": lengths.std(),
            "min": lengths.min(),
            "max": lengths.max(),
            "count": len(lengths)
        }
    
    def _display_results_section(self):
        """显示结果展示区域"""
        st.subheader("📈 结果展示")
        
        if st.session_state.workflow_results is None:
            st.info("📋 运行工作流后查看结果")
            return
        
        # 显示结果预览
        self._display_results_preview()
        
        # 显示分析图表
        self._display_analysis_charts()
    
    def _display_results_preview(self):
        """显示结果预览"""
        st.subheader("👀 结果预览")
        
        df = st.session_state.workflow_results
        
        # 显示基本信息
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("记录数", len(df))
        with col2:
            st.metric("列数", len(df.columns))
        with col3:
            st.metric("处理时间", "0.1s")  # 可以从工作流中获取真实时间
        
        # 显示数据
        with st.expander("查看结果数据"):
            st.dataframe(df.head(10), use_container_width=True)
    
    def _display_analysis_charts(self):
        """显示分析图表"""
        st.subheader("📊 分析图表")
        
        if not st.session_state.analysis_results:
            st.info("📈 没有分析结果")
            return
        
        # 根据分析结果显示不同的图表
        for analysis_type, results in st.session_state.analysis_results.items():
            if analysis_type == "text_quality_score":
                self._display_quality_score_chart(results)
            elif analysis_type == "text_length":
                self._display_text_length_chart(results)
    
    def _display_quality_score_chart(self, results: Dict[str, Any]):
        """显示质量分数图表"""
        with st.expander("文本质量分数分析", expanded=True):
            # 创建两列布局
            col1, col2 = st.columns(2)
            
            with col1:
                # 显示统计信息
                st.write("**统计信息:**")
                stats_df = pd.DataFrame({
                    "指标": ["平均分", "中位数", "标准差", "最小值", "最大值", "通过数量", "通过率"],
                    "值": [
                        f"{results['mean']:.2f}",
                        f"{results['median']:.2f}",
                        f"{results['std']:.2f}",
                        f"{results['min']:.2f}",
                        f"{results['max']:.2f}",
                        results['pass_count'],
                        f"{results['pass_rate']*100:.1f}%"
                    ]
                })
                st.dataframe(stats_df, use_container_width=True)
            
            with col2:
                # 显示饼图
                fig, ax = plt.subplots(figsize=(8, 6))
                labels = ['通过', '未通过']
                sizes = [results['pass_count'], results['count'] - results['pass_count']]
                colors = ['#4CAF50', '#FF5252']
                
                ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
                ax.axis('equal')
                ax.set_title('质量评估通过率')
                
                st.pyplot(fig)
                plt.close(fig)
            
            # 显示直方图
            st.write("**质量分数分布:**")
            df = st.session_state.workflow_results.to_pandas()
            scores = df["text_quality_score"]
            
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.hist(scores, bins=20, alpha=0.7, color='#2196F3')
            ax.axvline(0.5, color='red', linestyle='--', label='阈值')
            ax.set_xlabel('质量分数')
            ax.set_ylabel('频数')
            ax.set_title('质量分数分布直方图')
            ax.legend()
            
            st.pyplot(fig)
            plt.close(fig)
    
    def _display_text_length_chart(self, results: Dict[str, Any]):
        """显示文本长度图表"""
        with st.expander("文本长度分析", expanded=True):
            # 创建两列布局
            col1, col2 = st.columns(2)
            
            with col1:
                # 显示统计信息
                st.write("**统计信息:**")
                stats_df = pd.DataFrame({
                    "指标": ["平均长度", "中位数", "标准差", "最小值", "最大值"],
                    "值": [
                        f"{results['mean']:.2f}",
                        f"{results['median']:.2f}",
                        f"{results['std']:.2f}",
                        results['min'],
                        results['max']
                    ]
                })
                st.dataframe(stats_df, use_container_width=True)
            
            with col2:
                # 显示箱线图
                df = st.session_state.workflow_results.to_pandas()
                lengths = df["text_length"]
                
                fig, ax = plt.subplots(figsize=(8, 6))
                ax.boxplot(lengths)
                ax.set_ylabel('文本长度')
                ax.set_title('文本长度分布箱线图')
                
                st.pyplot(fig)
                plt.close(fig)
            
            # 显示直方图
            st.write("**文本长度分布:**")
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.hist(lengths, bins=30, alpha=0.7, color='#9C27B0')
            ax.set_xlabel('文本长度')
            ax.set_ylabel('频数')
            ax.set_title('文本长度分布直方图')
            
            st.pyplot(fig)
            plt.close(fig)
    
    def _add_log(self, action: str, message: str, level: str = "INFO"):
        """添加日志"""
        log_entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "action": action,
            "message": message,
            "level": level
        }
        
        st.session_state.processing_logs.append(log_entry)
    
    def _get_download_link(self, df: daft.DataFrame, filename: str, text: str):
        """获取下载链接"""
        # 转换为pandas DataFrame
        pd_df = df.to_pandas()
        
        # 创建CSV
        csv = pd_df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{text}</a>'
        return href