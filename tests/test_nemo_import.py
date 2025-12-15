# 测试NeMo Curator导入

try:
    print("尝试导入NeMo Curator模块...")
    from nemo_curator.datasets import DocumentDataset
    print("✓ 导入DocumentDataset成功")
    
    from nemo_curator.filters import (
        DocumentFilter,
        LanguageFilter,
        WordCountFilter,
        CharacterCountFilter,
        RepetitionFilter,
        QualityFilter
    )
    print("✓ 导入所有过滤器成功")
    
    from nemo_curator.utils.distributed_utils import get_client
    print("✓ 导入get_client成功")
    
    from nemo_curator.utils.script_utils import parse_client_args
    print("✓ 导入parse_client_args成功")
    
    print("\n🎉 所有NeMo Curator模块导入成功！")
except Exception as e:
    print(f"\n❌ 导入失败: {str(e)}")
    import traceback
    traceback.print_exc()