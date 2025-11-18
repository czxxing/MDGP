import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, Any, List


def setup_page():
    """设置页面配置"""
    st.set_page_config(
        page_title="多模态数据管理平台",
        page_icon="📊",
        layout="wide"
    )


def init_session_state():
    """初始化会话状态"""
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'current_dataframe' not in st.session_state:
        st.session_state.current_dataframe = None
    if 'files_info' not in st.session_state:
        st.session_state.files_info = []
    if 'active_tabs' not in st.session_state:
        st.session_state.active_tabs = ["数据目录", "数据处理", "数据统计"]
    if 'selected_tab' not in st.session_state:
        st.session_state.selected_tab = "数据目录"


def create_header():
    """创建头部区域"""
    with st.container():
        col1, col2 = st.columns([3, 1])
        with col1:
            st.title("多模态数据管理平台")
        with col2:
            st.write("📊 高效管理和处理多模态数据")
    st.divider()


def create_sidebar(lance_manager):
    """创建左侧导航栏"""
    with st.sidebar:
        st.header("导航菜单")
        
        # 导航选项
        if st.button("📂 数据目录", use_container_width=True, type="primary" if st.session_state.selected_tab == "数据目录" else "secondary"):
            st.session_state.selected_tab = "数据目录"
            st.rerun()
            
        if st.button("⚙️ 数据处理", use_container_width=True, type="primary" if st.session_state.selected_tab == "数据处理" else "secondary"):
            st.session_state.selected_tab = "数据处理"
            st.rerun()
            
        if st.button("📈 数据统计", use_container_width=True, type="primary" if st.session_state.selected_tab == "数据统计" else "secondary"):
            st.session_state.selected_tab = "数据统计"
            st.rerun()
        
        st.divider()
        
        # 数据库信息显示
        st.subheader("数据库信息")
        db_info = lance_manager.get_database_info()
        st.write(f"文件数量: {len(db_info['files'])}")
        st.write(f"数据库路径: {lance_manager.lance_file}")


def display_directory_content(data_dir: str, db_info: Dict[str, Any]):
    """显示数据目录内容"""
    st.header("数据目录结构")
    
    # 显示目录结构
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("本地数据目录")
        if st.button("扫描数据目录"):
            with st.spinner("正在扫描目录..."):
                from multimodal_processor.file_processor import scan_directory
                files_info = scan_directory(data_dir)
                st.session_state.files_info = files_info
                st.session_state.data_loaded = True
                st.success(f"找到 {len(files_info)} 个文件")
    
    with col2:
        st.subheader("数据库目录")
        if db_info["files"]:
            st.write(f"数据库中的文件:")
            for file_info in db_info["files"]:
                st.write(f"- {file_info['name']} ({file_info['size_mb']:.2f} MB)")
        else:
            st.info("数据库目录为空")


def display_processing_content(lance_manager):
    """显示数据处理内容"""
    st.header("数据处理")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("数据导入")
        if st.button("导入数据到数据库"):
            if hasattr(st.session_state, 'files_info') and st.session_state.files_info:
                with st.spinner("正在导入数据..."):
                    success = lance_manager.save_to_lance(st.session_state.files_info)
                    if success:
                        st.success("数据导入成功")
                        # 加载数据到会话状态
                        st.session_state.current_dataframe = lance_manager.load_from_lance()
            else:
                st.warning("请先扫描数据目录")
    
    with col2:
        st.subheader("数据导出")
        export_format = st.selectbox("选择导出格式", ["CSV", "JSON", "Parquet"])
        if st.button("导出数据"):
            if st.session_state.current_dataframe is not None:
                with st.spinner("正在导出数据..."):
                    try:
                        filepath = lance_manager.export_data(
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
            df = lance_manager.load_from_lance()
            if df is not None:
                st.session_state.current_dataframe = df
                st.dataframe(df.head(10))
            else:
                st.info("数据库中没有数据")


def plot_stats(stats: Dict[str, Any]):
    """绘制统计图表"""
    # 创建两列布局
    col1, col2 = st.columns(2)
    
    with col1:
        # 文件类型分布饼图
        fig, ax = plt.subplots(figsize=(8, 6))
        types = list(stats["type_counts"].keys())
        counts = list(stats["type_counts"].values())
        ax.pie(counts, labels=types, autopct='%1.1f%%', startangle=90)
        ax.axis('equal')
        ax.set_title('文件类型分布')
        st.pyplot(fig)
    
    with col2:
        # 文件大小按类型柱状图
        fig, ax = plt.subplots(figsize=(8, 6))
        types = list(stats["size_by_type"].keys())
        sizes = [s / (1024 * 1024) for s in list(stats["size_by_type"].values())]  # 转换为MB
        ax.bar(types, sizes)
        ax.set_xlabel('文件类型')
        ax.set_ylabel('大小 (MB)')
        ax.set_title('各类型文件大小分布')
        st.pyplot(fig)


def display_statistics_content():
    """显示数据统计内容"""
    st.header("数据统计")
    
    if st.button("生成统计信息"):
        if st.session_state.current_dataframe is not None:
            with st.spinner("正在生成统计信息..."):
                from multimodal_processor.file_processor import generate_stats
                stats = generate_stats(st.session_state.current_dataframe.to_dict('records'))
                
                # 显示基本统计
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("总文件数", stats["total_files"])
                with col2:
                    st.metric("总大小", f"{stats['total_size'] / (1024 * 1024):.2f} MB")
                with col3:
                    st.metric("文件类型数", len(stats["type_counts"]))
                
                # 显示详细统计
                st.subheader("详细统计")
                st.write("文件类型统计:")
                for file_type, count in stats["type_counts"].items():
                    st.write(f"- {file_type}: {count} 个文件")
                
                # 绘制图表
                plot_stats(stats)
        else:
            st.warning("请先加载数据")


def create_tabs_interface(data_dir: str, db_dir: str, lance_manager):
    """创建原生tabs界面"""
    # 创建tabs组件
    tab1, tab2, tab3 = st.tabs(["📂 数据目录", "⚙️ 数据处理", "📈 数据统计"])
    
    with tab1:
        db_info = lance_manager.get_database_info()
        display_directory_content(data_dir, db_info)
    
    with tab2:
        display_processing_content(lance_manager)
    
    with tab3:
        display_statistics_content()


def create_main_ui(data_dir: str, db_dir: str):
    """创建主界面
    
    Args:
        data_dir: 数据目录路径
        db_dir: 数据库目录路径
    """
    # 初始化Lance管理器
    from lance_db.lance_manager import LanceManager
    lance_manager = LanceManager(db_dir)
    
    # 创建头部
    create_header()
    
    # 创建左侧导航栏
    create_sidebar(lance_manager)
    
    # 创建原生tabs界面
    create_tabs_interface(data_dir, db_dir, lance_manager)


# 兼容旧的API调用
create_main_ui_old = create_main_ui