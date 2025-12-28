# 路由模块
import time
from flask import Blueprint, jsonify, render_template, make_response
from theme_fetcher import fetch_hot_themes, fetch_all_themes_with_stocks
from analyzer import analyze_and_format_stocks
from emotion_cycle import calculate_theme_emotion, get_stage_color, get_stage_advice
from theme_quality import evaluate_theme_quality
from news_fetcher import fetch_cls_news, evaluate_theme_news_factor, get_market_news_summary

try:
    import akshare as ak
except ImportError:
    ak = None

api = Blueprint('api', __name__)


def get_market_index_change() -> float:
    """获取大盘（上证指数）涨跌幅"""
    if ak is None:
        return 0
    try:
        df = ak.stock_zh_index_spot_em(symbol="上证指数")
        if df is not None and not df.empty:
            return float(df.iloc[0].get("涨跌幅", 0) or 0)
    except Exception as e:
        print(f"获取大盘数据失败: {e}")
    return 0


@api.route('/')
def index():
    """首页"""
    response = make_response(render_template('index.html'))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    response.headers['ETag'] = str(time.time())  # 每次生成新的ETag
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
        
        # 获取大盘涨跌幅（用于判断逆势）
        market_change = get_market_index_change()
        print(f"📈 大盘涨跌: {market_change:+.2f}%")
        
        # 并发获取所有数据
        theme_data = fetch_all_themes_with_stocks(theme_limit=8)
        
        # 预先获取新闻列表（避免重复请求）
        news_list = fetch_cls_news(50)
        market_news = get_market_news_summary()
        
        result = {}
        for theme_name, data in theme_data.items():
            stocks = data.get("stocks", [])
            theme_info = data.get("info", {})
            history = data.get("history", {})
            hot_score = data.get("hot_score", 0)
            
            # 板块涨跌幅
            theme_change = theme_info.get("change_pct", 0) or 0
            
            # 计算情绪周期
            emotion = calculate_theme_emotion(theme_info, stocks)
            
            # 打印分析日志
            print(f"\n【{theme_name}】热度:{hot_score:.0f}")
            print(f"  情绪: {emotion['stage']}({emotion['emotion_score']}分) | 涨跌:{theme_change:.2f}%")
            print(f"  指标: 涨停{emotion['metrics']['limit_up_count']}家 上涨率{emotion['metrics']['up_ratio']:.0f}% 振幅{emotion['metrics']['avg_amplitude']:.1f}%")
            if history.get('is_hot'):
                tags = history.get('fund_tags', [])
                print(f"  🔥资金认可: {', '.join(tags) if tags else '是'}")
            
            # 分析并格式化股票（传入大盘和板块涨跌幅）
            formatted_stocks = analyze_and_format_stocks(stocks, market_change, theme_change)
            
            # 调试：如果没有股票，打印原因
            if not formatted_stocks and stocks:
                print(f"  ⚠️ {theme_name} 有{len(stocks)}只原始股票但格式化后为空")
                for s in stocks[:3]:
                    print(f"    - {s.get('name')} price={s.get('price')} change={s.get('change_pct')}")
            
            # 打印龙头股和前排强度
            if formatted_stocks:
                print(f"  龙头: ", end="")
                top3 = []
                for s in formatted_stocks[:3]:
                    tags = []
                    if s.get('is_front_runner'):
                        tags = s.get('front_runner_tags', [])[:2]
                    tag_str = f"[{'|'.join(tags)}]" if tags else ""
                    top3.append(f"{s['name']}({s['change_pct']}){tag_str}")
                print(" | ".join(top3))
            
            # 资金认可标签
            fund_tags = []
            if history.get("continuous_up", 0) >= 2:
                fund_tags.append(f"连涨{history['continuous_up']}日")
            if history.get("continuous_inflow", 0) >= 2:
                fund_tags.append(f"连续{history['continuous_inflow']}日流入")
            if history.get("total_change_3d", 0) >= 5:
                fund_tags.append(f"3日涨{history['total_change_3d']:.1f}%")
            
            # 评估题材质量（大、新、强）
            quality = evaluate_theme_quality(theme_name, theme_info, stocks, history)
            
            # 评估消息面因子（传入股票列表用于匹配）
            news_factor = evaluate_theme_news_factor(theme_name, news_list, stocks)
            
            result[theme_name] = {
                "info": {
                    "change_pct": theme_change,
                    "up_count": theme_info.get("up_count", 0),
                    "down_count": theme_info.get("down_count", 0),
                },
                "history": {
                    "continuous_up": history.get("continuous_up", 0),
                    "continuous_inflow": history.get("continuous_inflow", 0),
                    "total_change_3d": round(history.get("total_change_3d", 0), 2),
                    "total_inflow_3d": round(history.get("total_inflow_3d", 0) / 100000000, 2),
                    "is_hot": history.get("is_hot", False),
                    "fund_tags": fund_tags,
                },
                "hot_score": hot_score,
                "quality": quality,
                "news": news_factor,
                "market_change": market_change,  # 大盘涨跌
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
            "data": sorted_result,
            "market_change": market_change
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500
