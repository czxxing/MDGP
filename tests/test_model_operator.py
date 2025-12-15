"""
测试模型调用包是否正常工作
"""

import sys
import os
# 将项目根目录添加到Python路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import daft
from mdgp_processors import ModelOperator

# 创建测试数据
data = {
    "text": [
        "介绍一下人工智能的发展历史",
        "什么是机器学习？",
        "解释一下深度学习的基本原理"
    ]
}

# 创建Daft数据框
dataframe = daft.from_pydict(data)

print("原始数据:")
print(dataframe.to_pydict())
print()

# 测试模型调用算子
# 注意：这里的示例需要实际的模型支持，可能需要根据实际情况调整
print("=== 测试模型调用算子 ===")
print("请注意：以下示例需要实际的模型支持，可能需要根据实际情况调整参数")
print()

# 示例1: 使用OpenAI兼容模型进行文本生成
print("示例1: 使用OpenAI兼容模型进行文本生成")
try:
    # 注意：这里需要设置正确的API密钥和基础URL
    model_operator = ModelOperator(
        task="generate",
        model_type="openai",
        model_name="gpt-3.5-turbo",
        text_column="text",
        output_column="generated_text",
        model_params={
            "api_key": "your-api-key",
            "base_url": "https://api.openai.com/v1"
        },
        task_params={
            "max_tokens": 100,
            "temperature": 0.7
        }
    )
    print("模型算子创建成功")
    # 实际调用可能需要真实的API密钥
    # result = model_operator.process(dataframe)
    # print("生成结果:")
    # print(result.to_pydict())
except Exception as e:
    print(f"测试失败: {e}")
    print("这是预期的，因为需要实际的API密钥")

print()

# 示例2: 使用Hugging Face模型进行文本生成
print("示例2: 使用Hugging Face模型进行文本生成")
try:
    model_operator = ModelOperator(
        task="generate",
        model_type="huggingface",
        model_name="gpt2",
        text_column="text",
        output_column="generated_text",
        task_params={
            "max_new_tokens": 50,
            "temperature": 0.7
        }
    )
    print("模型算子创建成功")
    # 实际调用可能需要下载模型
    # result = model_operator.process(dataframe)
    # print("生成结果:")
    # print(result.to_pydict())
except Exception as e:
    print(f"测试失败: {e}")
    print("这可能是因为Hugging Face Transformers库未安装，或者缺少模型")

print()

# 示例3: 使用本地模型进行文本生成
print("示例3: 使用本地模型进行文本生成")
try:
    model_operator = ModelOperator(
        task="generate",
        model_type="local",
        model_name="your-local-model",
        text_column="text",
        output_column="generated_text",
        model_params={
            "base_url": "http://localhost:8000"
        }
    )
    print("模型算子创建成功")
    # 实际调用需要本地模型服务运行
    # result = model_operator.process(dataframe)
    # print("生成结果:")
    # print(result.to_pydict())
except Exception as e:
    print(f"测试失败: {e}")
    print("这是预期的，因为需要本地模型服务运行")

print()
print("模型调用包测试完成！")