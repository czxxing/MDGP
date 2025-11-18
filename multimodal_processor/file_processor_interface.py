"""
文件处理接口定义模块
定义统一的文件处理抽象接口
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class FileProcessorInterface(ABC):
    """文件处理抽象接口"""
    
    @abstractmethod
    def scan_files(self, path: str) -> List[Dict[str, Any]]:
        """扫描指定路径的文件
        
        Args:
            path: 文件路径或S3桶路径
            
        Returns:
            文件信息列表
        """
        pass
    
    @abstractmethod
    def get_file_info(self, file_path: str) -> Optional[Dict[str, Any]]:
        """获取单个文件的详细信息
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件信息字典，如果文件不存在则返回None
        """
        pass
    
    @abstractmethod
    def validate_path(self, path: str) -> bool:
        """验证路径是否有效
        
        Args:
            path: 要验证的路径
            
        Returns:
            路径是否有效
        """
        pass
    
    @abstractmethod
    def get_supported_extensions(self) -> Dict[str, List[str]]:
        """获取支持的文件扩展名
        
        Returns:
            文件扩展名字典
        """
        pass