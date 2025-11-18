import streamlit as st
import pandas as pd
from typing import Optional


class HomePage:
    """首页页面类"""
    
    def __init__(self):
        """初始化首页页面"""
        pass
    
    def display(self):
        """显示首页内容"""
        st.title("欢迎使用多模态数据管理平台")
        st.write("这是一个功能强大的多模态数据管理平台，支持多种数据类型的处理和分析")
        
        # 平台介绍
        st.markdown("---")
        st.subheader("平台特色")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### 📊 多模态支持")
            st.write("支持文本、图像、音频等多种数据类型")
            st.markdown("")
        
        with col2:
            st.markdown("### 🔍 智能分析")
            st.write("提供数据统计、可视化分析和智能处理")
            st.markdown("")
        
        with col3:
            st.markdown("### 💾 高效存储")
            st.write("使用LanceDB进行高性能数据存储和检索")
            st.markdown("")
        
        # 快速开始指南
        st.markdown("---")
        st.subheader("快速开始")
        
        st.markdown("""
        1. **数据目录** - 浏览和管理本地数据文件
        2. **数据处理** - 导入数据到数据库或导出数据
        3. **数据统计** - 查看数据分析和可视化结果
        """)
        
        # 数据概览（如果数据已加载）
        if hasattr(st.session_state, 'current_dataframe') and st.session_state.current_dataframe is not None:
            st.markdown("---")
            st.subheader("数据概览")
            
            df = st.session_state.current_dataframe
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("总文件数", len(df))
            
            with col2:
                total_size = df['size'].sum() if 'size' in df.columns else 0
                st.metric("总大小", f"{total_size / (1024 * 1024):.2f} MB")
            
            with col3:
                file_types = df['file_type'].nunique() if 'file_type' in df.columns else 0
                st.metric("文件类型数", file_types)
        
        # 快速操作按钮
        st.markdown("---")
        st.subheader("快速操作")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📂 前往数据目录", use_container_width=True):
                st.session_state.selected_tab = "数据目录"
                st.rerun()
        
        with col2:
            if st.button("⚙️ 开始数据处理", use_container_width=True):
                st.session_state.selected_tab = "数据处理"
                st.rerun()
        
        with col3:
            if st.button("📈 查看数据统计", use_container_width=True):
                st.session_state.selected_tab = "数据统计"
                st.rerun()
    
    def get_title(self) -> str:
        """获取页面标题"""
        return "首页"
    
    def get_description(self) -> str:
        """获取页面描述"""
        return "平台概览和快速操作入口"