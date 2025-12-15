#!/usr/bin/env python3
"""
测试修改后的算子参数是否正常工作
"""

try:
    from mdgp_processors import (
        DataPipeline,
        # Readers
        CSVReader,
        # Filters
        TextLengthFilter,
        ImageResolutionFilter,
        AudioDurationFilter,
        QualityScoreFilter,
        # Evaluators
        TextQualityEvaluator,
        # Dedupers
        TextDeduper,
        # Writers
        CSVWriter,
    )
    
    print("✅ 所有类导入成功！")
    
    # 测试TextLengthFilter的text_column参数
    text_filter = TextLengthFilter(text_column="content", min_length=10, max_length=1000)
    print(f"✅ TextLengthFilter: text_column={text_filter.text_column}, min_length={text_filter.min_length}, max_length={text_filter.max_length}")
    
    # 测试ImageResolutionFilter的text_column参数
    image_filter = ImageResolutionFilter(text_column="caption", min_width=100, min_height=100)
    print(f"✅ ImageResolutionFilter: text_column={image_filter.text_column}, min_width={image_filter.min_width}, min_height={image_filter.min_height}")
    
    # 测试AudioDurationFilter的text_column参数
    audio_filter = AudioDurationFilter(text_column="transcript", min_duration=1.0, max_duration=60.0)
    print(f"✅ AudioDurationFilter: text_column={audio_filter.text_column}, min_duration={audio_filter.min_duration}, max_duration={audio_filter.max_duration}")
    
    # 测试QualityScoreFilter的text_column参数
    quality_filter = QualityScoreFilter(text_column="review", score_column="quality", min_score=0.5)
    print(f"✅ QualityScoreFilter: text_column={quality_filter.text_column}, score_column={quality_filter.score_column}, min_score={quality_filter.min_score}")
    
    # 测试TextQualityEvaluator的text_column参数（已存在）
    quality_evaluator = TextQualityEvaluator(text_column="comment", score_column="text_quality")
    print(f"✅ TextQualityEvaluator: text_column={quality_evaluator.text_column}, score_column={quality_evaluator.score_column}")
    
    # 测试TextDeduper的text_column参数（已存在）
    deduper = TextDeduper(text_column="message", keep="last")
    print(f"✅ TextDeduper: text_column={deduper.text_column}, keep={deduper.keep}")
    
    print("\n🎉 所有算子的text_column参数测试通过！")
    
except Exception as e:
    print(f"❌ 测试失败: {e}")