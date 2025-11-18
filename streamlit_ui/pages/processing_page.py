"""
数据处理页面模块
"""
import streamlit as st
from typing import Dict, Any


class ProcessingPage:
    """数据处理页面类"""
    
    def __init__(self, lance_manager):
        self.lance_manager = lance_manager
    
    def display(self):
        """显示数据处理内容"""
        st.header("数据处理")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("数据导入")
            if st.button("导入数据到数据库"):
                if hasattr(st.session_state, 'files_info') and st.session_state.files_info:
                    with st.spinner("正在导入数据..."):
                        success = self.lance_manager.save_to_lance(st.session_state.files_info)
                        if success:
                            st.success("数据导入成功")
                            # 加载数据到会话状态
                            st.session_state.current_dataframe = self.lance_manager.load_from_lance()
                else:
                    st.warning("请先扫描数据目录")
        
        with col2:
            st.subheader("数据导出")
            export_format = st.selectbox("选择导出格式", ["CSV", "JSON", "Parquet"])
            if st.button("导出数据"):
                if st.session_state.current_dataframe is not None:
                    with st.spinner("正在导出数据..."):
                        try:
                            filepath = self.lance_manager.export_data(
                                st.session_state.current_dataframe, 
                                export_format
                            )
                            st.success(f"数据导出成功: {filepath}")
                        except Exception as e:
                            st.error(f"导出失败: {str(e)}")
                else:
                    st.warning("请先导入数据")
        
        # 显示当前数据
        st.subheader("当前数据预览")
        if st.button("加载并显示数据"):
            with st.spinner("正在加载数据..."):
                df = self.lance_manager.load_from_lance()
                if df is not None:
                    st.session_state.current_dataframe = df
                    st.dataframe(df.head(10))
                else:
                    st.info("数据库中没有数据")
    
    def get_title(self) -> str:
        """获取页面标题"""
        return "数据处理"
    
    def get_description(self) -> str:
        """获取页面描述"""
        return "导入、导出和数据操作"