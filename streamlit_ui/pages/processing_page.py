"""
数据搜索页面模块
"""
import streamlit as st
import pandas as pd
import re
from typing import List, Dict, Any


class ProcessingPage:
    """数据搜索页面类"""
    
    def __init__(self, lance_manager):
        self.lance_manager = lance_manager
        
        # 初始化会话状态
        if 'search_results' not in st.session_state:
            st.session_state.search_results = None
        if 'search_query' not in st.session_state:
            st.session_state.search_query = ""
    
    def display(self):
        """显示数据搜索内容"""
        st.header("🔍 数据搜索")
        
        # 显示搜索表单
        self._display_search_form()
        
        # 显示搜索结果
        if st.session_state.search_results is not None:
            self._display_search_results()
    
    def _display_search_form(self):
        """显示搜索表单"""
        with st.form("search_form"):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.session_state.search_query = st.text_input(
                    "搜索关键词", 
                    value=st.session_state.search_query,
                    placeholder="输入搜索关键词..."
                )
            
            with col2:
                search_type = st.selectbox(
                    "搜索类型",
                    ["全文搜索", "精确匹配", "正则表达式"],
                    index=0
                )
            
            # 文件类型筛选
            file_types = st.multiselect(
                "文件类型筛选",
                ["text", "image", "audio", "video", "other"],
                default=[]
            )
            
            # 搜索按钮
            submitted = st.form_submit_button("搜索", type="primary")
            
            if submitted:
                self._perform_search(search_type, file_types)
    
    def _perform_search(self, search_type: str, file_types: List[str]):
        """执行搜索操作"""
        if not st.session_state.search_query:
            st.warning("请输入搜索关键词")
            return
        
        with st.spinner("正在搜索数据..."):
            # 从数据库加载数据
            df = self.lance_manager.load_from_lance()
            if df is None or df.empty:
                st.error("数据库中没有数据")
                return
            
            # 执行搜索
            results = self._search_data(df, search_type, file_types)
            
            if results.empty:
                st.info("未找到匹配的数据")
                st.session_state.search_results = None
            else:
                st.success(f"找到 {len(results)} 条匹配记录")
                st.session_state.search_results = results
    
    def _search_data(self, df: pd.DataFrame, search_type: str, file_types: List[str]) -> pd.DataFrame:
        """执行搜索逻辑"""
        # 复制数据以避免修改原始数据
        results = df.copy()
        
        # 文件类型筛选
        if file_types:
            results = results[results['file_type'].isin(file_types)]
        
        # 关键词搜索
        if st.session_state.search_query:
            query = st.session_state.search_query.lower()
            text_columns = [col for col in results.columns if results[col].dtype == 'object']
            
            # 根据搜索类型执行不同的搜索
            if search_type == "全文搜索":
                mask = results.apply(
                    lambda row: any(query in str(row[col]).lower() for col in text_columns),
                    axis=1
                )
            elif search_type == "精确匹配":
                mask = results.apply(
                    lambda row: any(str(row[col]).lower() == query for col in text_columns),
                    axis=1
                )
            else:  # 正则表达式
                try:
                    pattern = re.compile(query, re.IGNORECASE)
                    mask = results.apply(
                        lambda row: any(bool(pattern.search(str(row[col]))) for col in text_columns),
                        axis=1
                    )
                except re.error as e:
                    st.error(f"正则表达式错误: {e}")
                    return pd.DataFrame()
            
            results = results[mask]
        
        return results
    
    def _display_search_results(self):
        """显示搜索结果"""
        st.subheader(f"搜索结果 ({len(st.session_state.search_results)} 条)")
        
        # 显示搜索结果表格
        st.dataframe(st.session_state.search_results, use_container_width=True)
        
        # 导出选项
        if st.button("导出搜索结果"):
            csv = st.session_state.search_results.to_csv(index=False)
            st.download_button(
                label="下载CSV文件",
                data=csv,
                file_name=f"search_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    def get_title(self) -> str:
        """获取页面标题"""
        return "数据搜索"
    
    def get_description(self) -> str:
        """获取页面描述"""
        return "搜索和查询数据库中的多模态数据"