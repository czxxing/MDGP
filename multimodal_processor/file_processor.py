"""
文件处理工具模块
提供文件类型判断、目录扫描和统计功能的工具函数
"""

import os
from datetime import datetime
from typing import List, Dict, Any, Optional

# 支持的文件类型
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff']
VIDEO_EXTENSIONS = ['.mp4', '.avi', '.mov', '.wmv', '.flv']
AUDIO_EXTENSIONS = ['.mp3', '.wav', '.ogg', '.flac', '.aac']
TEXT_EXTENSIONS = ['.txt', '.csv', '.json', '.xml', '.md']


def get_file_type(filename: str) -> str:
    """获取文件类型
    
    Args:
        filename: 文件名
        
    Returns:
        文件类型字符串
    """
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


def create_file_processor(processor_type: str, **kwargs):
    """
    创建文件处理器实例的工厂函数
    
    Args:
        processor_type: 处理器类型 ('local' 或 's3')
        **kwargs: 处理器特定的参数
        
    Returns:
        文件处理器实例
    """
    if processor_type == 'local':
        from .local_file_processor import LocalFileProcessor
        return LocalFileProcessor()
    elif processor_type == 's3':
        from .s3_file_processor import S3FileProcessor
        return S3FileProcessor(**kwargs)
    else:
        raise ValueError(f"不支持的处理器类型: {processor_type}")


def scan_directory(directory: str) -> List[Dict[str, Any]]:
    """遍历目录获取文件信息
    
    Args:
        directory: 要扫描的目录路径
        
    Returns:
        文件信息列表
    """
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
                print(f"无法获取文件信息: {file_path}, 错误: {str(e)}")
    return files_info


def get_file_extensions() -> Dict[str, List[str]]:
    """获取所有支持的文件扩展名
    
    Returns:
        文件扩展名字典
    """
    return {
        "image": IMAGE_EXTENSIONS,
        "video": VIDEO_EXTENSIONS,
        "audio": AUDIO_EXTENSIONS,
        "text": TEXT_EXTENSIONS
    }


def generate_stats(files_info: List[Dict[str, Any]]) -> Dict[str, Any]:
    """生成文件统计信息
    
    Args:
        files_info: 文件信息列表
        
    Returns:
        统计信息字典
    """
    import pandas as pd
    
    df = pd.DataFrame(files_info)
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