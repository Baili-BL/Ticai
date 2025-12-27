# 路由模块
from flask import Blueprint, jsonify, render_template, make_response
from theme_fetcher import fetch_hot_themes, fetch_all_themes_with_stocks
from analyzer import analyze_and_format_stocks
from emotion_cycle import calculate_theme_emotion, get_stage_color, get_stage_advice

api = Blueprint('api', __name__)


@api.route('/')
def index():
    """首页"""
    response = make_response(render_template('index.html'))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@api.route('/api/themes')
def get_themes():
    """获取热门题材列表"""
    try:
        themes = fetch_hot_themes(10)
        return jsonify({
            "success": True,
            "data": themes
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@api.route('/api/all')
def get_all_data():
    """获取所有热门题材及其推荐股票（并发）"""
    try:
        print("\n" + "="*60)
        print("📊 开始获取热门题材数据...")
        print("="*60)
        
        # 并发获取所有数据
        theme_data = fetch_all_themes_with_stocks(theme_limit=8)
        
        result = {}
        for theme_name, data in theme_data.items():
            stocks = data.get("stocks", [])
            theme_info = data.get("info", {})
            history = data.get("history", {})
            hot_score = data.get("hot_score", 0)
            
            # 计算情绪周期
            emotion = calculate_theme_emotion(theme_info, stocks)
            
            # 打印分析日志
            print(f"\n【{theme_name}】热度:{hot_score:.0f}")
            print(f"  情绪: {emotion['stage']}({emotion['emotion_score']}分) | 涨跌:{theme_info.get('change_pct', 0):.2f}%")
            print(f"  指标: 涨停{emotion['metrics']['limit_up_count']}家 上涨率{emotion['metrics']['up_ratio']:.0f}% 振幅{emotion['metrics']['avg_amplitude']:.1f}%")
            if history.get('is_hot'):
                tags = history.get('fund_tags', [])
                print(f"  🔥资金认可: {', '.join(tags) if tags else '是'}")
            
            # 打印龙头股
            formatted_stocks = analyze_and_format_stocks(stocks)
            if formatted_stocks:
                print(f"  龙头: ", end="")
                top3 = [f"{s['name']}({s['change_pct']})" for s in formatted_stocks[:3]]
                print(" | ".join(top3))
            
            # 分析并格式化股票
            formatted_stocks = analyze_and_format_stocks(stocks)
            
            # 资金认可标签
            fund_tags = []
            if history.get("continuous_up", 0) >= 2:
                fund_tags.append(f"连涨{history['continuous_up']}日")
            if history.get("continuous_inflow", 0) >= 2:
                fund_tags.append(f"连续{history['continuous_inflow']}日流入")
            if history.get("total_change_3d", 0) >= 5:
                fund_tags.append(f"3日涨{history['total_change_3d']:.1f}%")
            
            result[theme_name] = {
                "info": {
                    "change_pct": theme_info.get("change_pct", 0),
                    "up_count": theme_info.get("up_count", 0),
                    "down_count": theme_info.get("down_count", 0),
                },
                "history": {
                    "continuous_up": history.get("continuous_up", 0),
                    "continuous_inflow": history.get("continuous_inflow", 0),
                    "total_change_3d": round(history.get("total_change_3d", 0), 2),
                    "total_inflow_3d": round(history.get("total_inflow_3d", 0) / 100000000, 2),  # 转为亿
                    "is_hot": history.get("is_hot", False),
                    "fund_tags": fund_tags,
                },
                "hot_score": hot_score,
                "emotion": {
                    "stage": emotion["stage"],
                    "stage_desc": emotion["stage_desc"],
                    "score": emotion["emotion_score"],
                    "color": get_stage_color(emotion["stage"]),
                    "advice": get_stage_advice(emotion["stage"]),
                    "metrics": emotion["metrics"],
                },
                "stocks": formatted_stocks
            }
        
        # 按热度分数排序
        sorted_result = dict(sorted(
            result.items(), 
            key=lambda x: x[1].get("hot_score", 0), 
            reverse=True
        ))
        
        print("\n" + "="*60)
        print(f"✅ 数据获取完成，共 {len(sorted_result)} 个题材")
        print("="*60 + "\n")
        
        return jsonify({
            "success": True,
            "data": sorted_result
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500
