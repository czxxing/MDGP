"""
数据目录页面模块
"""
import streamlit as st
from typing import Dict, Any
from multimodal_processor.file_processor import create_file_processor, get_file_type, get_file_extensions


class DirectoryPage:
    """数据目录页面类"""
    
    def __init__(self, lance_manager):
        self.lance_manager = lance_manager
        
        # 初始化会话状态
        if 'file_processor_type' not in st.session_state:
            st.session_state.file_processor_type = "local"
        if 'scanned_files' not in st.session_state:
            st.session_state.scanned_files = []
        if 'scan_path' not in st.session_state:
            st.session_state.scan_path = ""
    
    def display(self):
        """显示数据目录内容"""
        st.header("数据目录")
        
        # 文件路径扫描功能
        self._display_file_scan_section()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("数据导入")
            if st.button("导入数据到数据库"):
                if st.session_state.scanned_files:
                    with st.spinner("正在导入数据..."):
                        success = self.lance_manager.save_to_lance(st.session_state.scanned_files)
                        if success:
                            st.success("数据导入成功")
                            # 加载数据到会话状态
                            st.session_state.current_dataframe = self.lance_manager.load_from_lance()
                else:
                    st.warning("请先扫描文件路径")
        
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
    
    def _display_file_scan_section(self):
        """显示文件路径扫描区域"""
        st.subheader("文件路径扫描")
        
        # 选择文件处理器类型
        processor_type = st.radio(
            "选择文件来源:",
            ["本地文件", "S3桶"],
            index=0 if st.session_state.file_processor_type == "local" else 1,
            key="processor_type"
        )
        
        st.session_state.file_processor_type = "local" if processor_type == "本地文件" else "s3"
        
        # 根据选择的处理器类型显示不同的表单
        if st.session_state.file_processor_type == "local":
            self._display_local_file_form()
        else:
            self._display_s3_file_form()
        
        # 显示扫描结果
        if st.session_state.scanned_files:
            self._display_scan_results()
    
    def _display_local_file_form(self):
        """显示本地文件扫描表单"""
        st.write("扫描本地目录")
        
        # 路径输入
        path_input = st.text_input(
            "本地目录路径:",
            value=st.session_state.scan_path,
            placeholder="/path/to/your/directory",
            key="local_path"
        )
        
        if st.button("扫描目录"):
            if path_input:
                with st.spinner("正在扫描目录..."):
                    try:
                        processor = create_file_processor('local')
                        if processor.validate_path(path_input):
                            files = processor.scan_files(path_input)
                            st.session_state.scanned_files = files
                            st.session_state.scan_path = path_input
                            st.success(f"扫描完成！找到 {len(files)} 个文件")
                        else:
                            st.error("无效的目录路径，请检查路径是否正确")
                    except Exception as e:
                        st.error(f"扫描失败: {str(e)}")
            else:
                st.warning("请输入目录路径")
    
    def _display_s3_file_form(self):
        """显示S3文件扫描表单"""
        st.write("扫描S3桶")
        
        col1, col2 = st.columns(2)
        
        with col1:
            aws_access_key = st.text_input(
                "AWS Access Key ID (可选):",
                placeholder="AKIAIOSFODNN7EXAMPLE",
                key="aws_access_key"
            )
            
            aws_secret_key = st.text_input(
                "AWS Secret Access Key (可选):",
                type="password",
                placeholder="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                key="aws_secret_key"
            )
        
        with col2:
            region = st.text_input(
                "AWS Region:",
                value="us-east-1",
                placeholder="us-east-1",
                key="aws_region"
            )
            
            s3_path = st.text_input(
                "S3路径:",
                value=st.session_state.scan_path,
                placeholder="s3://bucket-name/path/",
                key="s3_path"
            )
        
        if st.button("扫描S3桶"):
            if s3_path:
                with st.spinner("正在扫描S3桶..."):
                    try:
                        processor = create_file_processor('s3',
                            aws_access_key_id=aws_access_key if aws_access_key else None,
                            aws_secret_access_key=aws_secret_key if aws_secret_key else None,
                            region_name=region
                        )
                        if processor.validate_path(s3_path):
                            files = processor.scan_files(s3_path)
                            st.session_state.scanned_files = files
                            st.session_state.scan_path = s3_path
                            st.success(f"扫描完成！找到 {len(files)} 个文件")
                        else:
                            st.error("无效的S3路径，请检查路径和凭证是否正确")
                    except Exception as e:
                        st.error(f"扫描失败: {str(e)}")
            else:
                st.warning("请输入S3路径")
    
    def _display_scan_results(self):
        """显示扫描结果"""
        st.subheader("扫描结果")
        
        # 统计信息
        files = st.session_state.scanned_files
        total_files = len(files)
        total_size = sum(f["size"] for f in files)
        
        # 按类型统计
        type_counts = {}
        for file in files:
            file_type = file["type"]
            type_counts[file_type] = type_counts.get(file_type, 0) + 1
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("总文件数", total_files)
        with col2:
            st.metric("总大小", f"{total_size / (1024*1024):.2f} MB")
        with col3:
            st.metric("数据源", files[0]["source"] if files else "无")
        
        # 文件类型分布
        st.write("文件类型分布:")
        for file_type, count in type_counts.items():
            st.write(f"- {file_type}: {count} 个文件")
        
        # 文件列表预览
        if st.checkbox("显示文件列表"):
            # 创建简化的文件列表显示
            file_data = []
            for file in files[:20]:  # 只显示前20个文件
                file_data.append({
                    "文件名": file["filename"],
                    "路径": file["path"],
                    "大小": f"{file['size'] / 1024:.2f} KB",
                    "类型": file["type"]
                })
            
            if file_data:
                st.dataframe(file_data)
            
            if len(files) > 20:
                st.info(f"还有 {len(files) - 20} 个文件未显示...")
    
    def get_title(self) -> str:
        """获取页面标题"""
        return "数据目录"
    
    def get_description(self) -> str:
        """获取页面描述"""
        return "文件扫描、导入、导出和数据管理"