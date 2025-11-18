"""
本地文件处理器
处理本地文件系统的文件扫描和操作
"""

import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from .file_processor_interface import FileProcessorInterface

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


class LocalFileProcessor(FileProcessorInterface):
    """本地文件处理类"""
    
    def scan_files(self, path: str) -> List[Dict[str, Any]]:
        """扫描本地目录获取文件信息
        
        Args:
            path: 本地目录路径
            
        Returns:
            文件信息列表
        """
        if not self.validate_path(path):
            raise ValueError(f"无效的路径: {path}")
        
        files_info = []
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    file_size = os.path.getsize(file_path)
                    created_time = os.path.getctime(file_path)
                    modified_time = os.path.getmtime(file_path)
                    file_type = get_file_type(file)
                    
                    files_info.append({
                        "filename": file,
                        "path": os.path.relpath(file_path, path),
                        "full_path": file_path,
                        "size": file_size,
                        "created_time": datetime.fromtimestamp(created_time).strftime('%Y-%m-%d %H:%M:%S'),
                        "modified_time": datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S'),
                        "type": file_type,
                        "source": "local"
                    })
                except Exception as e:
                    print(f"无法获取文件信息: {file_path}, 错误: {str(e)}")
        return files_info
    
    def get_file_info(self, file_path: str) -> Optional[Dict[str, Any]]:
        """获取单个本地文件的详细信息
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件信息字典，如果文件不存在则返回None
        """
        if not os.path.exists(file_path):
            return None
        
        try:
            file_size = os.path.getsize(file_path)
            created_time = os.path.getctime(file_path)
            modified_time = os.path.getmtime(file_path)
            filename = os.path.basename(file_path)
            file_type = get_file_type(filename)
            
            return {
                "filename": filename,
                "path": file_path,
                "size": file_size,
                "created_time": datetime.fromtimestamp(created_time).strftime('%Y-%m-%d %H:%M:%S'),
                "modified_time": datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S'),
                "type": file_type,
                "source": "local"
            }
        except Exception as e:
            print(f"无法获取文件信息: {file_path}, 错误: {str(e)}")
            return None
    
    def validate_path(self, path: str) -> bool:
        """验证本地路径是否有效
        
        Args:
            path: 本地路径
            
        Returns:
            路径是否有效
        """
        return os.path.exists(path) and os.path.isdir(path)
    
    def get_supported_extensions(self) -> Dict[str, List[str]]:
        """获取支持的文件扩展名
        
        Returns:
            文件扩展名字典
        """
        return {
            "image": IMAGE_EXTENSIONS,
            "video": VIDEO_EXTENSIONS,
            "audio": AUDIO_EXTENSIONS,
            "text": TEXT_EXTENSIONS
        }