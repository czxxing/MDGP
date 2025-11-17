import os
import pyarrow as pa
import lance
import pandas as pd
from typing import List, Dict, Any, Optional


class LanceManager:
    """Lance数据库管理器"""
    
    def __init__(self, db_path: str = "./db"):
        """初始化Lance管理器
        
        Args:
            db_path: 数据库目录路径
        """
        self.db_path = db_path
        self.lance_file = os.path.join(db_path, "multimodal_data.lance")
        
        # 确保数据库目录存在
        os.makedirs(db_path, exist_ok=True)
    
    def save_to_lance(self, files_info: List[Dict[str, Any]]) -> bool:
        """保存文件信息到Lance格式
        
        Args:
            files_info: 文件信息列表
            
        Returns:
            保存是否成功
        """
        try:
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
            
            # 写入Lance文件
            lance.write_table(table, self.lance_file, mode="append")
            return True
            
        except Exception as e:
            print(f"保存到Lance失败: {str(e)}")
            return False
    
    def load_from_lance(self) -> Optional[pd.DataFrame]:
        """从Lance加载数据
        
        Returns:
            DataFrame数据或None
        """
        try:
            if os.path.exists(self.lance_file):
                table = lance.dataset(self.lance_file)
                return table.to_pandas()
            return None
        except Exception as e:
            print(f"从Lance加载数据失败: {str(e)}")
            return None
    
    def export_data(self, df: pd.DataFrame, export_format: str, export_dir: str = None) -> str:
        """导出数据到指定格式
        
        Args:
            df: 要导出的DataFrame
            export_format: 导出格式 ("CSV", "JSON", "Parquet")
            export_dir: 导出目录，默认为数据库目录下的exports子目录
            
        Returns:
            导出文件路径
        """
        if export_dir is None:
            export_dir = os.path.join(self.db_path, "exports")
        
        os.makedirs(export_dir, exist_ok=True)
        
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"export_{timestamp}"
        
        if export_format == "CSV":
            filepath = os.path.join(export_dir, f"{filename}.csv")
            df.to_csv(filepath, index=False)
        elif export_format == "JSON":
            filepath = os.path.join(export_dir, f"{filename}.json")
            df.to_json(filepath, orient="records")
        elif export_format == "Parquet":
            filepath = os.path.join(export_dir, f"{filename}.parquet")
            df.to_parquet(filepath, index=False)
        else:
            raise ValueError(f"不支持的导出格式: {export_format}")
        
        return filepath
    
    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库信息
        
        Returns:
            数据库信息字典
        """
        info = {
            "db_path": self.db_path,
            "lance_file": self.lance_file,
            "exists": os.path.exists(self.lance_file),
            "files": []
        }
        
        if os.path.exists(self.db_path):
            for file in os.listdir(self.db_path):
                file_path = os.path.join(self.db_path, file)
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                    info["files"].append({
                        "name": file,
                        "size_mb": file_size
                    })
        
        return info
    
    def clear_database(self) -> bool:
        """清空数据库
        
        Returns:
            清空是否成功
        """
        try:
            if os.path.exists(self.lance_file):
                os.remove(self.lance_file)
            return True
        except Exception as e:
            print(f"清空数据库失败: {str(e)}")
            return False