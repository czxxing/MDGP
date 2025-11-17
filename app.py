import os
import sys
import streamlit as st
import pandas as pd
import daft
import lance
import pyarrow as pa
from PIL import Image
import matplotlib.pyplot as plt
import numpy as np
import time
from datetime import datetime

# 设置页面配置
st.set_page_config(
    page_title="多模态数据管理平台",
    page_icon="📊",
    layout="wide"
)

# 初始化会话状态
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'current_dataframe' not in st.session_state:
    st.session_state.current_dataframe = None

# 常量定义
DATA_DIR = "./data"
DB_DIR = "./db"

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

# 支持的文件类型
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff']
VIDEO_EXTENSIONS = ['.mp4', '.avi', '.mov', '.wmv', '.flv']
AUDIO_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.flac', '.aac']
TEXT_EXTENSIONS = ['.txt', '.csv', '.json', '.xml', '.md']

# 获取文件类型
@st.cache_data

def get_file_type(filename):
    ext = os.path.splitext(filename)[1].lower()
    if ext in IMAGE_EXTENSIONS:
        return "image"
    elif ext in VIDEO_EXTENSIONS:
        return "video"
    elif ext in AUDIO_EXTENSIONS:
        return "audio"
    elif ext in TEXT_EXTENSIONS:
        return "text"
    else:
        return "other"

# 遍历目录获取文件信息
@st.cache_data

def scan_directory(directory):
    files_info = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                file_size = os.path.getsize(file_path)
                created_time = os.path.getctime(file_path)
                modified_time = os.path.getmtime(file_path)
                file_type = get_file_type(file)
                
                files_info.append({
                    "filename": file,
                    "path": os.path.relpath(file_path, directory),
                    "size": file_size,
                    "created_time": datetime.fromtimestamp(created_time).strftime('%Y-%m-%d %H:%M:%S'),
                    "modified_time": datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S'),
                    "type": file_type
                })
            except Exception as e:
                st.warning(f"无法获取文件信息: {file_path}, 错误: {str(e)}")
    return files_info

# 保存到Lance格式

def save_to_lance(files_info, db_path):
    # 创建PyArrow表
    data = {
        "filename": [f["filename"] for f in files_info],
        "path": [f["path"] for f in files_info],
        "size": [f["size"] for f in files_info],
        "created_time": [f["created_time"] for f in files_info],
        "modified_time": [f["modified_time"] for f in files_info],
        "type": [f["type"] for f in files_info]
    }
    
    table = pa.Table.from_pydict(data)
    
    # 确保数据库目录存在
    os.makedirs(db_path, exist_ok=True)
    
    # 写入Lance文件
    lance.write_table(table, os.path.join(db_path, "multimodal_data.lance"), mode="overwrite")
    return True

# 从Lance加载数据

def load_from_lance(db_path):
    lance_file = os.path.join(db_path, "multimodal_data.lance")
    if os.path.exists(lance_file):
        table = lance.dataset(lance_file)
        return table.to_pandas()
    return None

# 生成文件统计信息

def generate_stats(df):
    stats = {}
    
    # 总文件数
    stats["total_files"] = len(df)
    
    # 按类型统计
    stats["type_counts"] = df["type"].value_counts().to_dict()
    
    # 按大小统计
    total_size = df["size"].sum()
    stats["total_size"] = total_size
    stats["size_by_type"] = df.groupby("type")["size"].sum().to_dict()
    
    return stats

# 绘制统计图表

def plot_stats(stats):
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

# 主应用逻辑
def main():
    st.title("多模态数据管理平台")
    
    # 创建导航选项卡
    tab1, tab2, tab3 = st.tabs(["数据目录", "数据处理", "数据统计"])
    
    with tab1:
        st.header("数据目录结构")
        
        # 显示目录结构
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("本地数据目录")
            if st.button("扫描数据目录"):
                with st.spinner("正在扫描目录..."):
                    files_info = scan_directory(DATA_DIR)
                    st.session_state.files_info = files_info
                    st.session_state.data_loaded = True
                    st.success(f"找到 {len(files_info)} 个文件")
        
        with col2:
            st.subheader("数据库目录")
            db_files = os.listdir(DB_DIR)
            if db_files:
                st.write(f"数据库中的文件:")
                for file in db_files:
                    file_path = os.path.join(DB_DIR, file)
                    file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                    st.write(f"- {file} ({file_size:.2f} MB)")
            else:
                st.info("数据库目录为空")
    
    with tab2:
        st.header("数据处理")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("数据导入")
            if st.button("导入数据到数据库"):
                if hasattr(st.session_state, 'files_info') and st.session_state.files_info:
                    with st.spinner("正在导入数据..."):
                        success = save_to_lance(st.session_state.files_info, DB_DIR)
                        if success:
                            st.success("数据导入成功")
                            # 加载数据到会话状态
                            st.session_state.current_dataframe = load_from_lance(DB_DIR)
                else:
                    st.warning("请先扫描数据目录")
        
        with col2:
            st.subheader("数据导出")
            export_format = st.selectbox("选择导出格式", ["CSV", "JSON", "Parquet"])
            if st.button("导出数据"):
                if st.session_state.current_dataframe is not None:
                    with st.spinner("正在导出数据..."):
                        export_dir = os.path.join(DB_DIR, "exports")
                        os.makedirs(export_dir, exist_ok=True)
                        
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"export_{timestamp}"
                        
                        if export_format == "CSV":
                            filepath = os.path.join(export_dir, f"{filename}.csv")
                            st.session_state.current_dataframe.to_csv(filepath, index=False)
                        elif export_format == "JSON":
                            filepath = os.path.join(export_dir, f"{filename}.json")
                            st.session_state.current_dataframe.to_json(filepath, orient="records")
                        elif export_format == "Parquet":
                            filepath = os.path.join(export_dir, f"{filename}.parquet")
                            st.session_state.current_dataframe.to_parquet(filepath, index=False)
                        
                        st.success(f"数据导出成功: {filepath}")
                else:
                    st.warning("请先导入数据")
        
        # 显示当前数据
        st.subheader("当前数据预览")
        if st.button("加载并显示数据"):
            with st.spinner("正在加载数据..."):
                df = load_from_lance(DB_DIR)
                if df is not None:
                    st.session_state.current_dataframe = df
                    st.dataframe(df.head(10))
                else:
                    st.info("数据库中没有数据")
    
    with tab3:
        st.header("数据统计")
        
        if st.button("生成统计信息"):
            if st.session_state.current_dataframe is not None:
                with st.spinner("正在生成统计信息..."):
                    stats = generate_stats(st.session_state.current_dataframe)
                    
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

if __name__ == "__main__":
    main()