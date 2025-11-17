import os
import sys

# 添加模块路径
sys.path.append(os.path.dirname(__file__))

import streamlit as st
from streamlit_ui.ui_components import setup_page, init_session_state, create_main_ui

# 常量定义
DATA_DIR = "./data"
DB_DIR = "./db"

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)


def main():
    """主应用逻辑"""
    # 设置页面配置
    setup_page()
    
    # 初始化会话状态
    init_session_state()
    
    # 创建主界面
    create_main_ui(DATA_DIR, DB_DIR)


if __name__ == "__main__":
    main()