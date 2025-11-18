"""
S3文件处理器
处理Amazon S3存储桶的文件扫描和操作
"""

import os
from typing import List, Dict, Any, Optional
import boto3
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


class S3FileProcessor(FileProcessorInterface):
    """S3桶文件处理类"""
    
    def __init__(self, aws_access_key_id: str = None, aws_secret_access_key: str = None, 
                 region_name: str = 'us-east-1'):
        """初始化S3处理器
        
        Args:
            aws_access_key_id: AWS访问密钥ID
            aws_secret_access_key: AWS秘密访问密钥
            region_name: AWS区域名称
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.s3_client = None
        
        # 初始化S3客户端
        self._init_s3_client()
    
    def _init_s3_client(self):
        """初始化S3客户端"""
        try:
            if self.aws_access_key_id and self.aws_secret_access_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name
                )
            else:
                # 使用默认凭证链
                self.s3_client = boto3.client('s3', region_name=self.region_name)
        except Exception as e:
            raise ValueError(f"无法初始化S3客户端: {str(e)}")
    
    def scan_files(self, s3_path: str) -> List[Dict[str, Any]]:
        """扫描S3桶获取文件信息
        
        Args:
            s3_path: S3路径，格式为 s3://bucket-name/path/
            
        Returns:
            文件信息列表
        """
        if not self.validate_path(s3_path):
            raise ValueError(f"无效的S3路径: {s3_path}")
        
        # 解析S3路径
        bucket_name, prefix = self._parse_s3_path(s3_path)
        
        files_info = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # 跳过目录对象
                        if obj['Key'].endswith('/'):
                            continue
                        
                        filename = os.path.basename(obj['Key'])
                        file_type = get_file_type(filename)
                        
                        files_info.append({
                            "filename": filename,
                            "path": obj['Key'],
                            "full_path": f"s3://{bucket_name}/{obj['Key']}",
                            "size": obj['Size'],
                            "created_time": obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                            "modified_time": obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                            "type": file_type,
                            "source": "s3"
                        })
        except Exception as e:
            raise ValueError(f"扫描S3桶失败: {str(e)}")
        
        return files_info
    
    def get_file_info(self, s3_path: str) -> Optional[Dict[str, Any]]:
        """获取单个S3文件的详细信息
        
        Args:
            s3_path: S3文件路径，格式为 s3://bucket-name/key
            
        Returns:
            文件信息字典，如果文件不存在则返回None
        """
        try:
            bucket_name, key = self._parse_s3_path(s3_path)
            
            # 获取文件元数据
            response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
            
            filename = os.path.basename(key)
            file_type = get_file_type(filename)
            
            return {
                "filename": filename,
                "path": key,
                "size": response['ContentLength'],
                "created_time": response['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                "modified_time": response['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                "type": file_type,
                "source": "s3"
            }
        except Exception as e:
            print(f"无法获取S3文件信息: {s3_path}, 错误: {str(e)}")
            return None
    
    def validate_path(self, s3_path: str) -> bool:
        """验证S3路径是否有效
        
        Args:
            s3_path: S3路径
            
        Returns:
            路径是否有效
        """
        try:
            if not s3_path.startswith('s3://'):
                return False
            
            bucket_name, prefix = self._parse_s3_path(s3_path)
            
            # 检查桶是否存在
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except Exception:
            return False
    
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
    
    def _parse_s3_path(self, s3_path: str) -> tuple:
        """解析S3路径
        
        Args:
            s3_path: S3路径
            
        Returns:
            (bucket_name, prefix) 元组
        """
        if not s3_path.startswith('s3://'):
            raise ValueError("S3路径必须以's3://'开头")
        
        path_parts = s3_path[5:].split('/', 1)
        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        return bucket_name, prefix