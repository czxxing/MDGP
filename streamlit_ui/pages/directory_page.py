"""
数据目录页面模块
"""
import streamlit as st
from typing import Dict, Any


class DirectoryPage:
    """数据目录页面类"""
    
    def __init__(self, data_dir: str, db_info: Dict[str, Any]):
        self.data_dir = data_dir
        self.db_info = db_info
    
    def display(self):
        """显示数据目录内容"""
        st.header("数据目录结构")
        
        # 显示目录结构
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("本地数据目录")
            if st.button("扫描数据目录"):
                with st.spinner("正在扫描目录..."):
                    from multimodal_processor.file_processor import scan_directory
                    files_info = scan_directory(self.data_dir)
                    st.session_state.files_info = files_info
                    st.session_state.data_loaded = True
                    st.success(f"找到 {len(files_info)} 个文件")
        
        with col2:
            st.subheader("数据库目录")
            if self.db_info["files"]:
                st.write(f"数据库中的文件:")
                for file_info in self.db_info["files"]:
                    st.write(f"- {file_info['name']} ({file_info['size_mb']:.2f} MB)")
            else:
                st.info("数据库目录为空")
    
    def get_title(self) -> str:
        """获取页面标题"""
        return "数据目录"
    
    def get_description(self) -> str:
        """获取页面描述"""
        return "数据目录和文件管理"