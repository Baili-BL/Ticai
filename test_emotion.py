# 测试情绪数据执行结果
from theme_fetcher import fetch_all_themes_with_stocks
from analyzer import analyze_and_format_stocks
from emotion_cycle import calculate_theme_emotion, get_stage_color, get_stage_advice

def main():
    print("=" * 60)
    print("正在获取热门题材数据...")
    print("=" * 60)
    
    # 获取数据
    theme_data = fetch_all_themes_with_stocks(theme_limit=5)
    
    if not theme_data:
        print("未获取到数据")
        return
    
    for theme_name, data in theme_data.items():
        stocks = data.get("stocks", [])
        theme_info = data.get("info", {})
        history = data.get("history", {})
        hot_score = data.get("hot_score", 0)
        
        # 计算情绪周期
        emotion = calculate_theme_emotion(theme_info, stocks)
        
        print(f"\n{'='*60}")
        print(f"【{theme_name}】 热度分: {hot_score}")
        print(f"{'='*60}")
        
        # 题材基本信息
        print(f"\n📊 题材信息:")
        print(f"   涨跌幅: {theme_info.get('change_pct', 0):.2f}%")
        print(f"   上涨: {theme_info.get('up_count', 0)} 家 | 下跌: {theme_info.get('down_count', 0)} 家")
        
        # 历史数据
        print(f"\n📈 资金认可:")
        print(f"   连续上涨: {history.get('continuous_up', 0)} 天")
        print(f"   连续流入: {history.get('continuous_inflow', 0)} 天")
        print(f"   3日累计涨幅: {history.get('total_change_3d', 0):.2f}%")
        print(f"   3日累计流入: {history.get('total_inflow_3d', 0)/100000000:.2f} 亿")
        print(f"   是否热门: {'✅ 是' if history.get('is_hot') else '❌ 否'}")
        
        # 情绪周期数据
        print(f"\n🎯 情绪周期:")
        print(f"   阶段: {emotion['stage']} ({emotion['stage_desc']})")
        print(f"   情绪分数: {emotion['emotion_score']}")
        print(f"   颜色: {get_stage_color(emotion['stage'])}")
        print(f"   建议: {get_stage_advice(emotion['stage'])}")
        
        # 情绪指标
        metrics = emotion.get("metrics", {})
        print(f"\n📉 情绪指标:")
        print(f"   涨跌幅: {metrics.get('change_pct', 0):.2f}%")
        print(f"   上涨比例: {metrics.get('up_ratio', 0):.1f}%")
        print(f"   涨停数: {metrics.get('limit_up_count', 0)}")
        print(f"   平均振幅: {metrics.get('avg_amplitude', 0):.2f}%")
        
        # 前3只股票
        formatted_stocks = analyze_and_format_stocks(stocks)
        if formatted_stocks:
            print(f"\n🔥 龙头股票 (前3):")
            for i, s in enumerate(formatted_stocks[:3], 1):
                print(f"   {i}. {s['name']}({s['code']}) {s['change_pct']} | 评分:{s['score']} | {s['signal']}")

if __name__ == "__main__":
    main()
