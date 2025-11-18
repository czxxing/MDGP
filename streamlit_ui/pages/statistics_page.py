"""
数据统计页面模块
"""
import streamlit as st
import matplotlib.pyplot as plt
from typing import Dict, Any

# 设置matplotlib支持中文显示
plt.rcParams['font.family'] = ['Noto Sans CJK JP', 'sans-serif']  # 使用系统中可用的Noto字体
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


class StatisticsPage:
    """数据统计页面类"""
    
    def __init__(self):
        pass
    
    def plot_stats(self, stats: Dict[str, Any]):
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
    
    def display(self):
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
                    self.plot_stats(stats)
            else:
                st.warning("请先加载数据")
    
    def get_title(self) -> str:
        """获取页面标题"""
        return "数据统计"
    
    def get_description(self) -> str:
        """获取页面描述"""
        return "数据分析和可视化"