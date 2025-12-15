"""
数据处理页面模块 - 实现算子拖拉拽和数据处理工作流构建
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, List, Optional
import logging
import json
import uuid
import base64
from io import BytesIO
from datetime import datetime

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
    """数据处理页面类 - 实现算子拖拉拽和工作流构建"""
    
    def __init__(self, lance_manager):
        self.lance_manager = lance_manager
        self.logger = self._setup_logging()
        
        # 初始化会话状态
        if 'current_dataframe' not in st.session_state:
            st.session_state.current_dataframe = None
        if 'workflow_operators' not in st.session_state:
            st.session_state.workflow_operators = []
        if 'workflow_connections' not in st.session_state:
            st.session_state.workflow_connections = []
        if 'workflow_results' not in st.session_state:
            st.session_state.workflow_results = None
        if 'processing_logs' not in st.session_state:
            st.session_state.processing_logs = []
        if 'analysis_results' not in st.session_state:
            st.session_state.analysis_results = {}
        if 'dragged_operator' not in st.session_state:
            st.session_state.dragged_operator = None
    
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
        tab1, tab2, tab3 = st.tabs(["🔧 工作流构建", "📥 数据加载", "📊 结果展示"])
        
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
        """显示工作流构建区域 - 实现算子拖拉拽"""
        st.subheader("🔧 工作流构建")
        
        # 算子库和工作流区域
        col1, col2 = st.columns([1, 3], gap="medium")
        
        with col1:
            st.subheader("🧩 算子库")
            self._display_operator_library()
        
        with col2:
            st.subheader("📋 工作流画布")
            self._display_workflow_canvas()
    
    def _display_operator_library(self):
        """显示算子库 - 支持拖拽"""
        # 算子分类
        operator_categories = {
            "读取器": [CSVReader, LanceReader, JSONReader, ParquetReader, ImageReader, AudioReader],
            "过滤器": [TextLengthFilter, ImageResolutionFilter, AudioDurationFilter, QualityScoreFilter],
            "去重器": [TextDeduper],
            "评估器": [TextQualityEvaluator],
            "写入器": [CSVWriter, LanceWriter]
        }
        
        for category, operators in operator_categories.items():
            with st.expander(f"{category}"):
                for operator_class in operators:
                    self._display_operator_item(operator_class)
    
    def _display_operator_item(self, operator_class):
        """显示单个算子项 - 支持拖拽"""
        # 获取算子名称和描述
        operator_name = operator_class.__name__
        
        # 使用特殊样式的按钮支持拖拽
        button_html = f"""
        <div style="margin: 8px 0; transition: all 0.3s ease;">
            <button 
                id="operator-{operator_name}" 
                class="stButton operator-btn" 
                style="width: 100%; padding: 12px 16px; cursor: grab; border: 2px solid #e0e0e0; border-radius: 8px; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); font-weight: 500; transition: all 0.2s ease;
                       box-shadow: 0 2px 4px rgba(0,0,0,0.1);"
                draggable="true"
                ondragstart="event.dataTransfer.setData('text/plain', '{operator_name}'); event.currentTarget.style.cursor = 'grabbing'; event.currentTarget.style.transform = 'scale(1.05)';"
                ondragend="event.currentTarget.style.cursor = 'grab'; event.currentTarget.style.transform = 'scale(1)';"
            >
                🧩 {operator_name}
            </button>
        </div>
        <style>
            .operator-btn:hover {
                border-color: #4CAF50 !important;
                box-shadow: 0 4px 8px rgba(0,0,0,0.15) !important;
                transform: translateY(-2px) !important;
            }
            .operator-btn:active {
                transform: scale(0.98) !important;
            }
        </style>
        """
        
        st.markdown(button_html, unsafe_allow_html=True)
        
        # 添加点击事件处理
        if st.button(f"➕ 添加 {operator_name}", key=f"add-{operator_name}", use_container_width=True):
            self._add_operator_to_workflow(operator_class)
    
    def _display_workflow_canvas(self):
        """显示工作流画布 - 支持拖拽放置"""
        # 添加拖拽区域的JavaScript
        drag_drop_js = """
        <script>
            // 获取工作流画布
            const canvas = document.getElementById('workflow-canvas');
            
            // 允许放置
            canvas.addEventListener('dragover', function(e) {
                e.preventDefault();
                canvas.style.border = '3px dashed #4CAF50';
                canvas.style.backgroundColor = 'rgba(76, 175, 80, 0.05)';
            });
            
            // 取消放置
            canvas.addEventListener('dragleave', function() {
                canvas.style.border = '2px dashed #ccc';
                canvas.style.backgroundColor = '#f9f9f9';
            });
            
            // 处理放置
            canvas.addEventListener('drop', function(e) {
                e.preventDefault();
                canvas.style.border = '2px dashed #ccc';
                canvas.style.backgroundColor = '#f9f9f9';
                
                // 获取拖拽的算子名称
                const operatorName = e.dataTransfer.getData('text/plain');
                
                // 创建一个隐藏的输入框来传递算子名称
                const hiddenInput = document.createElement('input');
                hiddenInput.type = 'hidden';
                hiddenInput.name = 'dropped_operator';
                hiddenInput.value = operatorName;
                
                // 创建一个隐藏的表单并提交
                const form = document.createElement('form');
                form.action = '';
                form.method = 'post';
                form.appendChild(hiddenInput);
                document.body.appendChild(form);
                form.submit();
            });
        </script>
        """
        
        # 创建工作流画布
        canvas_html = """
        <div 
            id="workflow-canvas" 
            style="
                height: 600px;
                border: 2px dashed #ccc;
                border-radius: 10px;
                padding: 20px;
                position: relative;
                background-color: #f9f9f9;
                background-image: linear-gradient(#e0e0e0 1px, transparent 1px),
                                  linear-gradient(90deg, #e0e0e0 1px, transparent 1px);
                background-size: 20px 20px;
                overflow-y: auto;
                box-shadow: inset 0 0 10px rgba(0,0,0,0.05);
            "
        >
            <div style="text-align: center; color: #666; margin-top: 200px;">
                <div style="font-size: 48px; margin-bottom: 10px;">📋</div>
                <h4 style="margin: 0;\ font-weight: 400;">拖拽算子到此处构建工作流</h4>
                <p style="margin: 5px 0; font-size: 14px; color: #999;">从左侧算子库拖拽算子到画布上</p>
            </div>
        </div>
        """
        
        # 显示工作流画布
        st.markdown(canvas_html, unsafe_allow_html=True)
        
        # 显示拖拽的JavaScript
        st.markdown(drag_drop_js, unsafe_allow_html=True)
        
        # 处理拖拽放置事件
        if "dropped_operator" in st.session_state:
            # 根据算子名称获取对应的类
            operator_class = self._get_operator_class_by_name(st.session_state.dropped_operator)
            if operator_class:
                self._add_operator_to_workflow(operator_class)
                # 清除会话状态
                del st.session_state.dropped_operator
        
        # 显示工作流中的算子
        if st.session_state.workflow_operators:
            # 显示工作流可视化图
            st.subheader("📊 工作流可视化")
            
            # 创建工作流流程图
            workflow_html = """
            <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 20px;">
                <div style="display: flex; flex-direction: column; gap: 15px; align-items: center;">
            """
            
            # 添加算子节点
            for i, operator_info in enumerate(st.session_state.workflow_operators):
                operator_name = operator_info["class"].__name__
                operator_type = "⚙️"
                if "Reader" in operator_name:
                    operator_type = "📥"
                elif "Writer" in operator_name:
                    operator_type = "📤"
                elif "Filter" in operator_name:
                    operator_type = "🔍"
                elif "Deduper" in operator_name:
                    operator_type = "🔄"
                elif "Evaluator" in operator_name:
                    operator_type = "📊"
                
                status = "✅" if operator_info.get("configured", False) else "❌"
                status_color = "#4CAF50" if operator_info.get("configured", False) else "#ff4444"
                
                workflow_html += f"""
                <div style="display: flex; align-items: center; gap: 10px; width: 100%; max-width: 600px;">
                    <div style="width: 40px; text-align: center; font-size: 24px;">{operator_type}</div>
                    <div style="flex: 1; padding: 15px; background-color: #f5f7fa; border: 2px solid #e0e0e0; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
                        <div style="font-weight: bold; display: flex; justify-content: space-between; align-items: center;">
                            <span>{i+1}. {operator_name}</span>
                            <span style="color: {status_color}; font-size: 18px;">{status}</span>
                        </div>
                        <div style="font-size: 12px; color: #666; margin-top: 5px;">
                            ID: {operator_info['id'][:8]}...
                        </div>
                    </div>
                </div>
                """
                
                # 添加连接线（最后一个算子不需要连接线）
                if i < len(st.session_state.workflow_operators) - 1:
                    workflow_html += f"""
                    <div style="width: 40px; height: 30px; display: flex; justify-content: center;">
                        <div style="width: 2px; background-color: #4CAF50; position: relative;">
                            <div style="position: absolute; top: 100%; left: -5px; width: 12px; height: 12px; border: 2px solid #4CAF50; border-radius: 50%; background-color: white;"></div>
                        </div>
                    </div>
                    """
            
            workflow_html += """
                </div>
            </div>
            """
            
            st.markdown(workflow_html, unsafe_allow_html=True)
            
            st.subheader("🔗 工作流算子")
            
            # 显示算子列表
            for i, operator_info in enumerate(st.session_state.workflow_operators):
                self._display_operator_card(i, operator_info)
            
            # 添加工作流控制按钮
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🚀 运行工作流", use_container_width=True, type="primary"):
                    self._run_workflow()
            
            with col2:
                if st.button("🗑️ 清除工作流", use_container_width=True, type="secondary"):
                    st.session_state.workflow_operators = []
                    st.session_state.workflow_connections = []
                    st.session_state.workflow_results = None
                    st.rerun()
        
        # 显示日志
        self._display_logs()
    
    def _get_operator_class_by_name(self, operator_name: str):
        """根据算子名称获取对应的类"""
        # 算子映射字典
        operator_map = {
            "CSVReader": CSVReader,
            "LanceReader": LanceReader,
            "JSONReader": JSONReader,
            "ParquetReader": ParquetReader,
            "ImageReader": ImageReader,
            "AudioReader": AudioReader,
            "CSVWriter": CSVWriter,
            "LanceWriter": LanceWriter,
            "TextLengthFilter": TextLengthFilter,
            "ImageResolutionFilter": ImageResolutionFilter,
            "AudioDurationFilter": AudioDurationFilter,
            "QualityScoreFilter": QualityScoreFilter,
            "TextDeduper": TextDeduper,
            "TextQualityEvaluator": TextQualityEvaluator
        }
        
        return operator_map.get(operator_name)
    
    def _add_operator_to_workflow(self, operator_class):
        """添加算子到工作流"""
        # 生成唯一ID
        operator_id = str(uuid.uuid4())
        
        # 获取算子参数信息
        params = self._get_operator_params(operator_class)
        
        # 对于需要必填参数的算子，不立即实例化
        # 而是在用户配置参数后再实例化
        operator = None
        
        # 保存算子信息
        operator_info = {
            "id": operator_id,
            "class_name": operator_class.__name__,
            "class": operator_class,  # 保存类引用
            "instance": operator,      # 实例化后再赋值
            "params": params,
            "position": {"x": 100, "y": 100},
            "configured": False       # 参数是否已配置
        }
        
        # 添加到工作流
        st.session_state.workflow_operators.append(operator_info)
        
        self._add_log("工作流构建", f"添加算子: {operator_class.__name__}")
    
    def _get_operator_params(self, operator_class):
        """获取算子参数信息"""
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
        elif operator_class == TextDeduper:
            params = {
                "text_column": "text",
                "threshold": 0.9
            }
        elif operator_class == LanceReader:
            params = {
                "file_path": "",
                "columns": None
            }
        elif operator_class == JSONReader:
            params = {
                "file_path": "",
                "encoding": "utf-8"
            }
        elif operator_class == ParquetReader:
            params = {
                "file_path": "",
                "columns": None
            }
        elif operator_class == ImageReader:
            params = {
                "file_path": ""
            }
        elif operator_class == AudioReader:
            params = {
                "file_path": ""
            }
        elif operator_class == LanceWriter:
            params = {
                "file_path": "",
                "mode": "append"
            }
        elif operator_class == ImageResolutionFilter:
            params = {
                "min_width": 0,
                "min_height": 0
            }
        elif operator_class == AudioDurationFilter:
            params = {
                "min_duration": 0,
                "max_duration": None
            }
        
        return params
    
    def _display_operator_card(self, index: int, operator_info: Dict[str, Any]):
        """显示算子卡片"""
        operator = operator_info["instance"]
        operator_class = operator_info["class"]
        params = operator_info["params"]
        
        # 获取算子名称
        operator_name = operator.name if operator else operator_class.__name__
        
        # 获取算子类型图标
        operator_type = "⚙️"
        if "Reader" in operator_name:
            operator_type = "📥"
        elif "Writer" in operator_name:
            operator_type = "📤"
        elif "Filter" in operator_name:
            operator_type = "🔍"
        elif "Deduper" in operator_name:
            operator_type = "🔄"
        elif "Evaluator" in operator_name:
            operator_type = "📊"
        
        # 检查算子是否已配置
        status_icon = "❌" if not operator_info.get("configured", False) else "✅"
        status_color = "#ff4444" if not operator_info.get("configured", False) else "#4CAF50"
        
        with st.expander(f"{operator_type} {index+1}. {operator_name} <span style='color: {status_color}; font-weight: bold;'>{status_icon}</span>", expanded=True):
            # 显示算子状态提示
            if not operator_info.get("configured", False):
                st.warning("⚠️ 算子未配置，请完成参数设置")
            else:
                st.success("✅ 算子已配置")
            
            # 显示算子参数配置
            st.markdown("<div style='margin: 10px 0; padding: 15px; background-color: #f5f5f5; border-radius: 8px;'>", unsafe_allow_html=True)
            self._display_operator_params(operator, operator_class, params, operator_info)
            st.markdown("</div>", unsafe_allow_html=True)
            
            # 更新参数
            operator_info["params"] = params
            
            # 添加删除按钮
            delete_col, _, _ = st.columns([1, 2, 2])
            with delete_col:
                if st.button(f"❌ 删除", key=f"delete_{operator_info['id']}"):
                    st.session_state.workflow_operators.pop(index)
                    # 删除相关连接
                    st.session_state.workflow_connections = [
                        conn for conn in st.session_state.workflow_connections 
                        if conn["source"] != operator_info["id"] and conn["target"] != operator_info["id"]
                    ]
                    st.rerun()
    
    def _display_operator_params(self, operator: Operator, operator_class, params: Dict[str, Any], operator_info: Dict[str, Any]):
        """显示算子参数配置"""
        # 根据算子类型显示不同的参数配置
        if operator_class == TextLengthFilter or (operator and isinstance(operator, TextLengthFilter)):
            params["text_column"] = st.text_input("文本列名", value=params["text_column"])
            params["min_length"] = st.number_input("最小长度", min_value=0, value=params["min_length"])
            params["max_length"] = st.number_input("最大长度", min_value=0, value=params["max_length"] or 1000, step=1)
        
        elif operator_class == TextQualityEvaluator or (operator and isinstance(operator, TextQualityEvaluator)):
            params["text_column"] = st.text_input("文本列名", value=params["text_column"])
            params["score_column"] = st.text_input("分数列名", value=params["score_column"])
        
        elif operator_class == QualityScoreFilter or (operator and isinstance(operator, QualityScoreFilter)):
            params["score_column"] = st.text_input("分数列名", value=params["score_column"])
            params["threshold"] = st.slider("质量阈值", min_value=0.0, max_value=1.0, value=params["threshold"])
        
        elif operator_class == CSVReader or (operator and isinstance(operator, CSVReader)):
            params["file_path"] = st.text_input("文件路径", value=params["file_path"])
            params["delimiter"] = st.text_input("分隔符", value=params["delimiter"])
        
        elif operator_class == CSVWriter or (operator and isinstance(operator, CSVWriter)):
            params["file_path"] = st.text_input("文件路径", value=params["file_path"])
            params["delimiter"] = st.text_input("分隔符", value=params["delimiter"])
        
        elif operator_class == TextDeduper or (operator and isinstance(operator, TextDeduper)):
            params["text_column"] = st.text_input("文本列名", value=params["text_column"])
            params["threshold"] = st.slider("相似度阈值", min_value=0.0, max_value=1.0, value=params["threshold"])
        
        # 添加配置完成按钮
        col1, col2 = st.columns([2, 1])
        with col2:
            if st.button("💾 保存配置", key=f"save_{operator_class.__name__}_{operator_info['id']}"):
                self._configure_operator(operator_class, params, operator_info)
    
    def _configure_operator(self, operator_class, params, operator_info):
        """配置算子参数并实例化"""
        try:
            # 检查必填参数
            if operator_class in [CSVReader, JSONReader, ParquetReader, ImageReader, AudioReader, LanceReader] and not params.get("file_path"):
                st.error(f"❌ {operator_class.__name__} 需要配置文件路径参数")
                return
            elif operator_class in [CSVWriter, LanceWriter] and not params.get("file_path"):
                st.error(f"❌ {operator_class.__name__} 需要配置文件路径参数")
                return
            
            # 实例化算子
            operator = operator_class(**params)
            
            # 更新算子信息
            operator_info["instance"] = operator
            operator_info["configured"] = True
            
            st.success("✅ 算子配置完成")
            self._add_log("算子配置", f"{operator_class.__name__} 配置完成")
            
        except Exception as e:
            st.error(f"❌ 算子配置失败: {str(e)}")
            self._add_log("算子配置", f"{operator_class.__name__} 配置失败: {str(e)}", "ERROR")
    
    def _run_workflow(self):
        """运行工作流"""
        if not st.session_state.workflow_operators:
            st.error("❌ 工作流为空，请添加算子")
            return
        
        if st.session_state.current_dataframe is None:
            st.error("❌ 没有加载数据，请先加载数据")
            return
        
        # 检查所有算子是否已配置
        for i, operator_info in enumerate(st.session_state.workflow_operators):
            if not operator_info.get("configured", False):
                st.error(f"❌ 算子 {i+1} 未配置完成，请检查参数设置")
                return
        
        with st.spinner("正在运行工作流..."):
            try:
                # 转换数据格式
                df = st.session_state.current_dataframe
                
                # 初始化管道
                pipeline = DataPipeline()
                pipeline.set_input(df)
                
                # 创建日志区域
                log_container = st.empty()
                logs = []
                
                # 添加算子到管道
                for i, operator_info in enumerate(st.session_state.workflow_operators):
                    operator = operator_info["instance"]
                    operator_class = operator_info["class"]
                    
                    # 添加到管道
                    pipeline.add_operator(operator)
                    
                    # 更新日志
                    logs.append(f"✅ 添加算子: {operator_class.__name__}")
                    log_container.text_area("运行日志", "\n".join(logs), height=100)
                
                # 运行管道
                logs.append("🚀 开始执行工作流...")
                log_container.text_area("运行日志", "\n".join(logs), height=100)
                
                result_df = pipeline.run()
                
                # 更新日志
                logs.append(f"✅ 工作流执行完成！")
                log_container.text_area("运行日志", "\n".join(logs), height=100)
                
                # 保存结果
                st.session_state.workflow_results = result_df
                st.success(f"✅ 工作流运行完成！结果包含 {len(result_df)} 条记录")
                
                self._add_log("工作流运行", f"工作流运行完成，结果包含 {len(result_df)} 条记录")
                
                # 分析结果
                logs.append("📊 开始分析结果...")
                log_container.text_area("运行日志", "\n".join(logs), height=100)
                
                self._analyze_workflow_results(result_df)
                
                logs.append("✅ 结果分析完成！")
                log_container.text_area("运行日志", "\n".join(logs), height=100)
                
            except Exception as e:
                st.error(f"❌ 工作流运行失败: {str(e)}")
                self._add_log("工作流运行", f"工作流运行失败: {str(e)}", "ERROR")
    
    def _analyze_workflow_results(self, result_df: pd.DataFrame):
        """分析工作流结果"""
        try:
            analysis_results = {}
            
            # 基本统计信息
            analysis_results["basic_stats"] = {
                "records_count": len(result_df),
                "columns_count": len(result_df.columns),
                "columns": list(result_df.columns)
            }
            
            # 文本列分析
            text_columns = result_df.select_dtypes(include=["object"]).columns
            if text_columns.any():
                text_analysis = {}
                for col in text_columns:
                    # 计算文本长度统计
                    text_lengths = result_df[col].str.len()
                    text_analysis[col] = {
                        "min_length": text_lengths.min(),
                        "max_length": text_lengths.max(),
                        "mean_length": text_lengths.mean(),
                        "median_length": text_lengths.median()
                    }
                analysis_results["text_analysis"] = text_analysis
            
            # 数值列分析
            numeric_columns = result_df.select_dtypes(include=["int", "float"]).columns
            if numeric_columns.any():
                numeric_analysis = {}
                for col in numeric_columns:
                    numeric_analysis[col] = {
                        "min": result_df[col].min(),
                        "max": result_df[col].max(),
                        "mean": result_df[col].mean(),
                        "median": result_df[col].median(),
                        "std": result_df[col].std()
                    }
                analysis_results["numeric_analysis"] = numeric_analysis
            
            # 缺失值分析
            missing_values = result_df.isnull().sum()
            if missing_values.any():
                analysis_results["missing_values"] = missing_values.to_dict()
            
            # 保存分析结果
            st.session_state.analysis_results = analysis_results
            
        except Exception as e:
            st.error(f"❌ 结果分析失败: {str(e)}")
            self._add_log("结果分析", f"结果分析失败: {str(e)}", "ERROR")
    
    def _display_results_section(self):
        """显示结果区域"""
        st.subheader("📊 分析结果")
        
        if st.session_state.workflow_results is None:
            st.info("🔄 请先运行工作流")
            return
        
        # 显示结果预览
        self._display_results_preview()
        
        # 显示分析结果
        if st.session_state.analysis_results:
            self._display_analysis_results()
        
        # 提供结果下载
        self._display_results_download()
    
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
            st.metric("数据类型", f"{len(df.select_dtypes(include=['object']).columns)}文本列")
        
        # 显示前几行数据
        with st.expander("查看数据详情"):
            st.dataframe(df.head(10), use_container_width=True)
    
    def _display_analysis_results(self):
        """显示分析结果"""
        analysis = st.session_state.analysis_results
        
        # 基本统计信息
        st.subheader("📋 基本统计")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("总记录数", analysis["basic_stats"]["records_count"])
        with col2:
            st.metric("总列数", analysis["basic_stats"]["columns_count"])
        
        # 文本列分析
        if "text_analysis" in analysis:
            st.subheader("📝 文本列分析")
            for col, stats in analysis["text_analysis"].items():
                with st.expander(f"列: {col}"):
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("最小长度", stats["min_length"])
                    with col2:
                        st.metric("最大长度", stats["max_length"])
                    with col3:
                        st.metric("平均长度", round(stats["mean_length"], 2))
                    with col4:
                        st.metric("中位数长度", stats["median_length"])
                    
                    # 绘制文本长度分布图
                    fig, ax = plt.subplots(figsize=(10, 4))
                    df = st.session_state.workflow_results
                    text_lengths = df[col].str.len()
                    sns.histplot(text_lengths, kde=True, ax=ax)
                    ax.set_title(f"文本长度分布 - {col}")
                    ax.set_xlabel("文本长度")
                    ax.set_ylabel("频率")
                    st.pyplot(fig)
        
        # 数值列分析
        if "numeric_analysis" in analysis:
            st.subheader("📈 数值列分析")
            for col, stats in analysis["numeric_analysis"].items():
                with st.expander(f"列: {col}"):
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("最小值", round(stats["min"], 2))
                    with col2:
                        st.metric("最大值", round(stats["max"], 2))
                    with col3:
                        st.metric("平均值", round(stats["mean"], 2))
                    with col4:
                        st.metric("中位数", round(stats["median"], 2))
                    with col5:
                        st.metric("标准差", round(stats["std"], 2))
                    
                    # 绘制数值分布直方图
                    fig, ax = plt.subplots(figsize=(10, 4))
                    df = st.session_state.workflow_results
                    sns.histplot(df[col], kde=True, ax=ax)
                    ax.set_title(f"数值分布 - {col}")
                    ax.set_xlabel(col)
                    ax.set_ylabel("频率")
                    st.pyplot(fig)
                    
                    # 绘制箱线图
                    fig, ax = plt.subplots(figsize=(10, 4))
                    sns.boxplot(x=df[col], ax=ax)
                    ax.set_title(f"箱线图 - {col}")
                    st.pyplot(fig)
        
        # 缺失值分析
        if "missing_values" in analysis:
            st.subheader("🔍 缺失值分析")
            missing_df = pd.DataFrame({
                "列名": list(analysis["missing_values"].keys()),
                "缺失值数量": list(analysis["missing_values"].values())
            })
            missing_df["缺失值比例"] = (missing_df["缺失值数量"] / len(st.session_state.workflow_results) * 100).round(2)
            
            st.dataframe(missing_df, use_container_width=True)
            
            # 绘制缺失值柱状图
            fig, ax = plt.subplots(figsize=(12, 6))
            missing_df.plot(kind="bar", x="列名", y="缺失值数量", ax=ax)
            ax.set_title("各列缺失值数量")
            ax.set_xlabel("列名")
            ax.set_ylabel("缺失值数量")
            plt.xticks(rotation=45)
            st.pyplot(fig)
    
    def _display_results_download(self):
        """显示结果下载选项"""
        st.subheader("💾 结果下载")
        
        df = st.session_state.workflow_results
        
        # 生成下载链接
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 下载为CSV",
            data=csv,
            file_name="workflow_results.csv",
            mime="text/csv"
        )
        
        # 生成Excel下载链接
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Results", index=False)
        excel_buffer.seek(0)
        st.download_button(
            label="📥 下载为Excel",
            data=excel_buffer,
            file_name="workflow_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    def _add_log(self, action: str, message: str, level: str = "INFO"):
        """添加日志记录"""
        log_entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "action": action,
            "message": message,
            "level": level
        }
        
        # 添加到会话状态
        st.session_state.processing_logs.append(log_entry)
        
        # 保持日志长度限制
        if len(st.session_state.processing_logs) > 100:
            st.session_state.processing_logs.pop(0)
    
    def _display_logs(self):
        """显示日志"""
        if st.session_state.processing_logs:
            st.subheader("📝 操作日志")
            
            with st.expander("查看日志", expanded=False):
                for log in st.session_state.processing_logs:
                    # 根据日志级别显示不同颜色
                    if log["level"] == "ERROR":
                        st.markdown(f"📅 {log['timestamp']} - ❌ {log['action']}: {log['message']}")
                    elif log["level"] == "WARNING":
                        st.markdown(f"📅 {log['timestamp']} - ⚠️ {log['action']}: {log['message']}")
                    else:
                        st.markdown(f"📅 {log['timestamp']} - ✅ {log['action']}: {log['message']}")