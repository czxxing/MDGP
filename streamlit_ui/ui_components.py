import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, Any, List

# 导入页面模块
from streamlit_ui.pages import DirectoryPage, ProcessingPage, StatisticsPage, HomePage, DataProcessingPage

def setup_page():
    """设置页面配置"""
    st.set_page_config(
        page_title="多模态数据管理平台",
        page_icon="📊",
        layout="centered"
    )

def init_session_state():
    """初始化会话状态"""
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'current_dataframe' not in st.session_state:
        st.session_state.current_dataframe = None
    if 'files_info' not in st.session_state:
        st.session_state.files_info = []

def create_header():
    """创建占据整个头部的页面区域"""
    # 添加CSS来使头部占据整个宽度
    st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(135deg, #1a73e8 0%, #0d47a1 100%);
        color: white;
        padding: 2rem 1rem;
        margin: -1rem -1rem 1rem -1rem;
        border-radius: 0;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    }
    .main-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
    }
    .main-header p {
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
        opacity: 0.9;
    }
    .main-header .container {
        max-width: 100%;
        margin: 0 auto;
    }
    /* 移除Streamlit的默认边距 */
    .block-container {
        padding-top: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # 创建全宽头部
    st.markdown("""
    <div class="main-header">
        <div class="container">
            <h1>多模态数据管理平台</h1>
            <p>📊 高效管理和处理多模态数据</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

def open_tab(tab_name: str):
    """打开指定的tab"""
    if tab_name not in st.session_state.active_tabs:
        st.session_state.active_tabs.append(tab_name)
    st.session_state.selected_tab = tab_name
    st.rerun()

def close_tab(tab_name: str):
    """关闭指定的tab"""
    if tab_name in st.session_state.active_tabs:
        st.session_state.active_tabs.remove(tab_name)
        # 如果关闭的是当前选中的tab，切换到首页
        if st.session_state.selected_tab == tab_name:
            st.session_state.selected_tab = "首页"
        st.rerun()

def create_sidebar(lance_manager):
    """创建左侧导航栏"""
    with st.sidebar:
        st.header("导航菜单")
        
        # 导航选项 - 现在使用open_tab函数来打开tab
        if st.button("🏠 首页", use_container_width=True, type="primary" if st.session_state.selected_tab == "首页" else "secondary"):
            open_tab("首页")
            
        if st.button("📂 数据目录", use_container_width=True, type="primary" if st.session_state.selected_tab == "数据目录" else "secondary"):
            open_tab("数据目录")
            
        if st.button("🔍 数据搜索", use_container_width=True, type="primary" if st.session_state.selected_tab == "数据搜索" else "secondary"):
            open_tab("数据搜索")
            
        if st.button("⚙️ 数据处理", use_container_width=True, type="primary" if st.session_state.selected_tab == "数据处理" else "secondary"):
            open_tab("数据处理")
            
        if st.button("📈 数据统计", use_container_width=True, type="primary" if st.session_state.selected_tab == "数据统计" else "secondary"):
            open_tab("数据统计")
        
        st.divider()
        
        # 显示当前激活的tab
        st.subheader("已打开的标签页")
        for tab_name in st.session_state.active_tabs:
            if tab_name != "首页":  # 首页不能关闭
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"📄 {tab_name}")
                with col2:
                    if st.button("✕", key=f"close_{tab_name}", help=f"关闭{tab_name}"):
                        close_tab(tab_name)
        
        st.divider()
        
        # 数据库信息显示
        st.subheader("数据库信息")
        db_info = lance_manager.get_database_info()
        st.write(f"文件数量: {len(db_info['files'])}")
        st.write(f"数据库路径: {lance_manager.lance_file}")



def display_tab_content(tab_name: str, data_dir: str, db_dir: str, lance_manager):
    """显示选中tab的内容"""
    # 使用现代化的容器样式
    with st.container():
        if tab_name == "首页":
            page = HomePage()
            page.display()
        elif tab_name == "数据目录":
            page = DirectoryPage(lance_manager)
            page.display()
        elif tab_name == "数据搜索":
            page = ProcessingPage(lance_manager)
            page.display()
        elif tab_name == "数据处理":
            page = DataProcessingPage(lance_manager)
            page.display()
        elif tab_name == "数据统计":
            page = StatisticsPage()
            page.display()

def create_main_ui(data_dir: str, db_dir: str):
    """创建主界面
    
    Args:
        data_dir: 数据目录路径
        db_dir: 数据库目录路径
    """
    # 初始化Lance管理器
    from lance_db.lance_manager import LanceManager
    lance_manager = LanceManager(db_dir)
    
    # 创建全宽头部 - 确保在最上方
    create_header()
    
    # 直接显示所有tab页，不使用侧边栏导航
    # 定义所有tab页的标签
    tab_labels = ["🏠 首页", "📂 数据目录", "🔍 数据搜索", "⚙️ 数据处理", "📈 数据统计"]
    tab_names = ["首页", "数据目录", "数据搜索", "数据处理", "数据统计"]
    
    # 创建tab组件，显示所有tab页
    tabs = st.tabs(tab_labels)
    
    # 在每个tab中显示对应内容
    for i, tab_name in enumerate(tab_names):
        with tabs[i]:
            display_tab_content(tab_name, data_dir, db_dir, lance_manager)

# 兼容旧的API调用
create_main_ui_old = create_main_ui