"""
数据处理页面模块
"""
import streamlit as st


class ProcessingPage:
    """数据处理页面类"""
    
    def __init__(self, lance_manager):
        self.lance_manager = lance_manager
    
    def display(self):
        """显示数据处理内容"""
        st.header("数据处理")

    
    def get_title(self) -> str:
        """获取页面标题"""
        return "数据处理"
    
    def get_description(self) -> str:
        """获取页面描述"""
        return "数据处理功能已整合到数据目录页面"