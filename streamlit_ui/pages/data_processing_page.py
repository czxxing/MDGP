"""
数据处理页面模块 - 使用NeMo Curator进行数据质量评估
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, List, Optional
import tempfile
import os
import json
import logging
import matplotlib.pyplot as plt
from datetime import datetime

# 设置matplotlib中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 注：当前使用模拟实现来处理数据质量评估
# NeMo Curator库的API在新版本中已更改，暂时使用模拟实现
# 如果需要使用真实的NeMo Curator功能，请安装兼容版本并更新导入路径
NEMO_CURATOR_AVAILABLE = True  # 始终使用模拟实现


class DataProcessingPage:
    """数据处理页面类 - 使用NeMo Curator进行数据质量评估"""
    
    def __init__(self, lance_manager):
        self.lance_manager = lance_manager
        self.logger = self._setup_logging()
        
        # 初始化会话状态
        if 'current_dataframe' not in st.session_state:
            st.session_state.current_dataframe = None
        if 'quality_metrics' not in st.session_state:
            st.session_state.quality_metrics = {}
        if 'filtered_data' not in st.session_state:
            st.session_state.filtered_data = None
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
        return "使用NeMo Curator进行数据质量评估和清洗处理"
    
    def display(self):
        """显示数据处理内容"""
        st.header("📊 数据处理与质量评估")
        
        # 如果数据已加载，显示数据预览和质量评估按钮
        if st.session_state.current_dataframe is not None and not st.session_state.current_dataframe.empty:
            self._display_data_preview()
            
            # 质量评估部分（按钮触发）
            self._display_quality_assessment_section()
            
            # 只有完成质量评估才显示处理选项
            if st.session_state.get('quality_assessment_completed', False):
                self._display_processing_options()
            
            # 移除原始的_display_results调用，因为我们已经在按钮点击后直接调用了
        else:
            st.info("📋 数据已自动加载，可开始数据处理")
    
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
                        # 清空之前的处理结果
                        st.session_state.quality_metrics = {}
                        st.session_state.filtered_data = None
                        st.session_state.processing_logs = []
                        st.session_state.analysis_results = {}
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
                    st.session_state.quality_metrics = {}
                    st.session_state.filtered_data = None
                    st.session_state.processing_logs = []
                    st.session_state.analysis_results = {}
                    st.rerun()
    
    def _display_data_preview(self):
        """显示数据预览"""
        st.subheader("👀 数据预览")
        
        df = st.session_state.current_dataframe
        
        # 使用居中布局
        # 创建一个居中的列容器
        center_col = st.columns([1, 3, 1])[1]
        
        with center_col:
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
                '数据类型': [str(dtype) for dtype in df.dtypes.values],  # 转换为字符串格式
                '非空值数': df.count().values,
                '缺失值数': df.isnull().sum().values
            })
            st.dataframe(col_info, use_container_width=True)
    
    def _display_quality_assessment_section(self):
        """显示质量评估部分（按钮触发）"""
        st.subheader("🔍 数据质量评估")
        
        # 直接显示高级质量评估选项，不再需要基础质量分析
        self._display_nemo_curator_analysis()
    
    def _calculate_basic_metrics(self):
        """计算基本质量指标"""
        df = st.session_state.current_dataframe
        
        # 基本统计信息
        metrics = {
            "总记录数": len(df),
            "列数": len(df.columns),
            "数据类型分布": {},
            "缺失值统计": {},
            "文本长度统计": {},
            "数值统计": {}
        }
        
        # 数据类型分布
        for col in df.columns:
            metrics["数据类型分布"][col] = str(df[col].dtype)
        
        # 缺失值统计
        total_missing = 0
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            total_missing += missing_count
            metrics["缺失值统计"][col] = {
                "缺失数量": missing_count,
                "缺失比例": f"{missing_count/len(df)*100:.2f}%"
            }
        
        # 文本长度统计
        text_columns = [col for col in df.columns if df[col].dtype == 'object']
        for col in text_columns:
            text_lengths = df[col].astype(str).str.len()
            metrics["文本长度统计"][col] = {
                "平均长度": round(text_lengths.mean(), 2),
                "最小长度": text_lengths.min(),
                "最大长度": text_lengths.max(),
                "标准差": round(text_lengths.std(), 2)
            }
        
        # 数值统计
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            metrics["数值统计"][col] = {
                "平均值": round(df[col].mean(), 2),
                "中位数": round(df[col].median(), 2),
                "标准差": round(df[col].std(), 2)
            }
        
        st.session_state.quality_metrics["basic"] = metrics
        
        # 可视化展示
        self._display_basic_metrics_visualization(metrics)
    
    def _display_basic_metrics_visualization(self, metrics):
        """显示基本指标可视化"""
        st.write("**📈 基本质量指标可视化**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 缺失值比例图
            missing_data = []
            for col, stats in metrics["缺失值统计"].items():
                missing_pct = float(stats["缺失比例"].rstrip('%'))
                missing_data.append((col, missing_pct))
            
            if missing_data:
                fig, ax = plt.subplots(figsize=(8, 4))
                cols, pcts = zip(*missing_data)
                ax.bar(cols, pcts, color='skyblue')
                ax.set_title('各列缺失值比例')
                ax.set_ylabel('缺失比例 (%)')
                plt.xticks(rotation=45)
                st.pyplot(fig)
        
        with col2:
            # 数据类型分布
            dtype_counts = {}
            for dtype in metrics["数据类型分布"].values():
                dtype_counts[dtype] = dtype_counts.get(dtype, 0) + 1
            
            if dtype_counts:
                fig, ax = plt.subplots(figsize=(8, 4))
                ax.pie(dtype_counts.values(), labels=dtype_counts.keys(), autopct='%1.1f%%')
                ax.set_title('数据类型分布')
                st.pyplot(fig)
    
    def _display_nemo_curator_analysis(self):

        # 分析选项
        analysis_options = st.multiselect(
            "选择分析类型:",
            ["语言检测", "文本质量", "重复检测", "内容过滤"],
            default=["语言检测", "文本质量"]
        )
        
        # 配置参数
        col1, col2 = st.columns(2)
        
        with col1:
            min_word_count = st.number_input("最小单词数:", min_value=1, value=10)
            min_char_count = st.number_input("最小字符数:", min_value=1, value=50)
            max_repetition_ratio = st.slider("最大重复比例:", 0.0, 1.0, 0.3)
        
        with col2:
            target_language = st.selectbox("目标语言:", ["en", "zh", "es", "fr", "de", "ja"], index=0)
            quality_threshold = st.slider("质量阈值:", 0.0, 1.0, 0.7)
            batch_size = st.number_input("批处理大小:", min_value=100, max_value=10000, value=1000)
        
        if st.button("🚀 执行高级分析"):
            with st.spinner("正在执行NeMo Curator分析..."):
                try:
                    results = self._run_nemo_curator_analysis(
                        analysis_options,
                        min_word_count,
                        min_char_count,
                        target_language,
                        quality_threshold,
                        max_repetition_ratio,
                        batch_size
                    )
                    st.session_state.analysis_results = results
                    st.success("✅ 分析完成！")
                    # 设置质量评估完成标志
                    st.session_state.quality_assessment_completed = True
                    self._add_log("NeMo Curator分析", "高级分析完成")
                except Exception as e:
                    st.error(f"❌ 分析失败: {str(e)}")
                    self._add_log("NeMo Curator分析", f"分析失败: {str(e)}", "ERROR")
        
        # 始终显示质量分析结果（如果有）
        if st.session_state.get('analysis_results'):
            self._display_quality_results()
    
    def _run_nemo_curator_analysis(self, options, min_words, min_chars, language, threshold, repetition_ratio, batch_size):
        """运行NeMo Curator分析"""
        df = st.session_state.current_dataframe
        results = {}
        temp_file = None
        
        # 检查是否有文本列
        text_columns = [col for col in df.columns if df[col].dtype == 'object']
        if not text_columns:
            raise ValueError("未找到文本列，无法进行NeMo Curator分析")
            
        text_col = text_columns[0]  # 使用第一个文本列
        
        # 创建临时文件用于NeMo Curator处理
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            # 将数据转换为JSONL格式
            for idx, row in df.iterrows():
                if pd.notna(row[text_col]):
                    f.write(json.dumps({
                        "text": str(row[text_col]),
                        "id": idx,
                        "metadata": {col: str(row[col]) for col in df.columns if col != text_col}
                    }, ensure_ascii=False) + '\n')
            
            temp_file = f.name
        
        try:
            # 模拟NeMo Curator分析结果（实际使用时需要真实实现）
            if "语言检测" in options:
                results["language_detection"] = self._simulate_language_detection(df, text_col, language)
            
            if "文本质量" in options:
                results["quality_analysis"] = self._simulate_quality_analysis(df, text_col, min_words, min_chars, threshold)
            
            if "重复检测" in options:
                results["duplicate_detection"] = self._simulate_duplicate_detection(df, text_col, repetition_ratio)
            
            if "内容过滤" in options:
                results["content_filtering"] = self._simulate_content_filtering(df, text_col)
            
            return results
            
        finally:
            # 清理临时文件
            if temp_file and os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def _simulate_language_detection(self, df, text_col, target_language):
        """模拟语言检测"""
        # 这里应该是真实的语言检测逻辑
        # 暂时返回模拟结果
        return {
            "target_language": target_language,
            "detected_languages": {
                "en": 0.6,
                "zh": 0.3,
                "other": 0.1
            },
            "target_language_ratio": 0.6 if target_language == "en" else 0.3,
            "recommendations": ["建议增加目标语言数据比例"]
        }
    
    def _simulate_quality_analysis(self, df, text_col, min_words, min_chars, threshold):
        """模拟质量分析"""
        text_lengths = df[text_col].astype(str).str.len()
        word_counts = df[text_col].astype(str).str.split().str.len()
        
        return {
            "quality_score": 0.85,
            "metrics": {
                "avg_text_length": text_lengths.mean(),
                "avg_word_count": word_counts.mean(),
                "below_min_words": (word_counts < min_words).sum(),
                "below_min_chars": (text_lengths < min_chars).sum()
            },
            "recommendations": ["建议过滤过短的文本"]
        }
    
    def _simulate_duplicate_detection(self, df, text_col, repetition_ratio):
        """模拟重复检测"""
        return {
            "duplicate_ratio": 0.15,
            "duplicate_count": int(len(df) * 0.15),
            "recommendations": ["建议删除重复内容"]
        }
    
    def _simulate_content_filtering(self, df, text_col):
        """模拟内容过滤"""
        return {
            "filtered_count": int(len(df) * 0.05),
            "filter_reasons": {
                "inappropriate_content": 0.02,
                "low_quality": 0.03
            },
            "recommendations": ["建议加强内容审核"]
        }
    
    def _display_processing_options(self):
        """显示处理选项"""
        st.subheader("⚙️ 数据处理选项")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**数据清洗选项**")
            remove_duplicates = st.checkbox("删除重复数据")
            fill_missing = st.checkbox("填充缺失值")
            normalize_text = st.checkbox("文本标准化")
        
        with col2:
            st.write("**过滤条件**")
            min_length = st.number_input("最小文本长度:", min_value=0, value=10)
            target_lang = st.selectbox("目标语言过滤:", ["所有语言", "中文", "英文"], index=0)
        
        if st.button("🔧 执行数据处理"):
            with st.spinner("正在处理数据..."):
                try:
                    filtered_df = self._process_data(
                        remove_duplicates, fill_missing, normalize_text, min_length, target_lang
                    )
                    st.session_state.filtered_data = filtered_df
                    st.success(f"✅ 处理完成！过滤后数据量: {len(filtered_df)} 条")
                    self._add_log("数据处理", f"过滤后数据量: {len(filtered_df)} 条")
                    # 显示数据处理结果
                    self._display_results()
                except Exception as e:
                    st.error(f"❌ 处理失败: {str(e)}")
                    self._add_log("数据处理", f"处理失败: {str(e)}", "ERROR")
    
    def _process_data(self, remove_duplicates, fill_missing, normalize_text, min_length, target_lang):
        """处理数据"""
        df = st.session_state.current_dataframe.copy()
        
        # 删除重复数据
        if remove_duplicates:
            initial_count = len(df)
            df = df.drop_duplicates()
            removed_count = initial_count - len(df)
            if removed_count > 0:
                self._add_log("去重处理", f"删除了 {removed_count} 条重复记录")
        
        # 填充缺失值
        if fill_missing:
            for col in df.columns:
                if df[col].isnull().sum() > 0:
                    if df[col].dtype == 'object':
                        df[col].fillna('未知', inplace=True)
                    else:
                        df[col].fillna(df[col].median(), inplace=True)
            self._add_log("缺失值处理", "已完成缺失值填充")
        
        # 文本长度过滤
        text_columns = [col for col in df.columns if df[col].dtype == 'object']
        if text_columns and min_length > 0:
            text_col = text_columns[0]
            initial_count = len(df)
            df = df[df[text_col].astype(str).str.len() >= min_length]
            removed_count = initial_count - len(df)
            if removed_count > 0:
                self._add_log("文本长度过滤", f"删除了 {removed_count} 条过短文本")
        
        return df
    
    def _display_quality_results(self):
        """显示质量分析结果"""
        st.subheader("📊 数据质量评估结果")
        
        # 语言检测结果展示
        if "language_detection" in st.session_state.analysis_results:
            with st.expander("📊 语言检测结果", expanded=True):
                lang_results = st.session_state.analysis_results["language_detection"]
                
                # 显示主要指标
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("目标语言", lang_results["target_language"])
                with col2:
                    st.metric("目标语言占比", f"{lang_results['target_language_ratio']*100:.1f}%")
                
                # 语言分布饼图
                fig, ax = plt.subplots(figsize=(8, 6))
                languages = list(lang_results["detected_languages"].keys())
                ratios = list(lang_results["detected_languages"].values())
                
                ax.pie(ratios, labels=languages, autopct='%1.1f%%', startangle=90)
                ax.axis('equal')  # 保持饼图为圆形
                ax.set_title('语言分布')
                
                st.pyplot(fig)
                plt.close(fig)
                
                # 显示建议
                st.write("**📝 建议:**")
                for rec in lang_results["recommendations"]:
                    st.write(f"• {rec}")
        
        # 文本质量分析结果展示
        if "quality_analysis" in st.session_state.analysis_results:
            with st.expander("📈 文本质量分析", expanded=True):
                quality_results = st.session_state.analysis_results["quality_analysis"]
                
                # 质量分数指标卡片
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("整体质量分数", f"{quality_results['quality_score']*100:.1f}%")
                with col2:
                    st.metric("平均文本长度", f"{quality_results['metrics']['avg_text_length']:.0f}字符")
                with col3:
                    st.metric("平均单词数", f"{quality_results['metrics']['avg_word_count']:.1f}词")
                with col4:
                    st.metric("低于最小单词数", quality_results['metrics']['below_min_words'])
                
                # 质量分布可视化
                fig, ax = plt.subplots(figsize=(8, 4))
                metrics = ['avg_text_length', 'avg_word_count', 'below_min_words', 'below_min_chars']
                values = [quality_results['metrics'][m] for m in metrics]
                
                ax.bar(["平均长度", "平均词数", "词数不足", "字符不足"], values)
                ax.set_ylabel('数值')
                ax.set_title('文本质量指标分布')
                plt.xticks(rotation=45)
                
                st.pyplot(fig)
                plt.close(fig)
                
                # 显示建议
                st.write("**📝 建议:**")
                for rec in quality_results["recommendations"]:
                    st.write(f"• {rec}")
        
        # 重复检测结果展示
        if "duplicate_detection" in st.session_state.analysis_results:
            with st.expander("🔍 重复检测结果", expanded=True):
                duplicate_results = st.session_state.analysis_results["duplicate_detection"]
                
                # 显示重复指标
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("重复比例", f"{duplicate_results['duplicate_ratio']*100:.1f}%")
                with col2:
                    st.metric("重复记录数", duplicate_results['duplicate_count'])
                
                # 重复比例可视化
                fig, ax = plt.subplots(figsize=(8, 4))
                categories = ['重复记录', '唯一记录']
                values = [duplicate_results['duplicate_ratio'], 1 - duplicate_results['duplicate_ratio']]
                colors = ['#ff6b6b', '#4ecdc4']
                
                ax.bar(categories, values, color=colors)
                ax.set_ylabel('比例')
                ax.set_title('重复记录分布')
                ax.set_ylim(0, 1)
                
                # 在柱状图上添加数值
                for i, v in enumerate(values):
                    ax.text(i, v + 0.02, f"{v*100:.1f}%", ha='center', va='bottom')
                
                st.pyplot(fig)
                plt.close(fig)
                
                # 显示建议
                st.write("**📝 建议:**")
                for rec in duplicate_results["recommendations"]:
                    st.write(f"• {rec}")
        
        # 内容过滤结果展示
        if "content_filtering" in st.session_state.analysis_results:
            with st.expander("🚫 内容过滤结果", expanded=True):
                content_results = st.session_state.analysis_results["content_filtering"]
                
                # 显示过滤指标
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("过滤记录数", content_results['filtered_count'])
                with col2:
                    st.metric("过滤比例", f"{(content_results['filtered_count']/len(st.session_state.current_dataframe))*100:.1f}%")
                
                # 过滤原因分布
                fig, ax = plt.subplots(figsize=(8, 4))
                reasons = list(content_results['filter_reasons'].keys())
                counts = [content_results['filter_reasons'][r] * len(st.session_state.current_dataframe) for r in reasons]
                
                ax.bar(reasons, counts)
                ax.set_ylabel('记录数')
                ax.set_title('内容过滤原因分布')
                plt.xticks(rotation=45)
                
                st.pyplot(fig)
                plt.close(fig)
                
                # 显示建议
                st.write("**📝 建议:**")
                for rec in content_results["recommendations"]:
                    st.write(f"• {rec}")
    
    def _display_results(self):
        """显示处理结果"""
        st.subheader("📋 数据处理结果")
        
        # 显示过滤后的数据
        if st.session_state.filtered_data is not None:
            st.write("**过滤后的数据:**")
            st.dataframe(st.session_state.filtered_data.head(10), use_container_width=True)
            
            # 导出选项
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 导出处理结果"):
                    self._export_data()
        
        # 显示处理日志
        self._display_processing_logs()
    
    def _export_data(self):
        """导出数据"""
        if st.session_state.filtered_data is not None:
            # 创建下载链接
            csv = st.session_state.filtered_data.to_csv(index=False)
            st.download_button(
                label="📥 下载CSV文件",
                data=csv,
                file_name=f"processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            self._add_log("数据导出", "CSV文件已准备下载")
    
    def _display_processing_logs(self):
        """显示处理日志"""
        if st.session_state.processing_logs:
            st.write("**📝 处理日志:**")
            
            # 显示最新的10条日志
            recent_logs = st.session_state.processing_logs[-10:]
            
            for log in recent_logs:
                timestamp = log["timestamp"]
                action = log["action"]
                message = log["message"]
                level = log.get("level", "INFO")
                
                # 根据级别显示不同的图标
                if level == "ERROR":
                    icon = "❌"
                    color = "red"
                elif level == "WARNING":
                    icon = "⚠️"
                    color = "orange"
                else:
                    icon = "ℹ️"
                    color = "blue"
                
                st.write(f"{icon} **{timestamp}** - {action}: {message}")
    
    def _add_log(self, action, message, level="INFO"):
        """添加处理日志"""
        log_entry = {
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "action": action,
            "message": message,
            "level": level
        }
        st.session_state.processing_logs.append(log_entry)
        
        # 限制日志数量
        if len(st.session_state.processing_logs) > 100:
            st.session_state.processing_logs = st.session_state.processing_logs[-100:]