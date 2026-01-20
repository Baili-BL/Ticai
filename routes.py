# 路由模块
import time
from datetime import date, datetime
from flask import Blueprint, jsonify, render_template, make_response, request
from theme_fetcher import fetch_hot_themes, fetch_all_themes_with_stocks
from analyzer import analyze_and_format_stocks
from emotion_cycle import calculate_theme_emotion, get_stage_color, get_stage_advice
from theme_quality import evaluate_theme_quality
from news_fetcher import fetch_cls_news, evaluate_theme_news_factor, get_market_news_summary
from database import (
    save_report, get_report_by_date, get_recent_reports,
    get_performance_summary, get_stock_history, init_database
)
from performance_tracker import update_all_performance, get_today_performance_report

try:
    import akshare as ak
except ImportError:
    ak = None

api = Blueprint('api', __name__)

# 确保数据库已初始化
init_database()


def get_market_index_change() -> float:
    """获取大盘（上证指数）涨跌幅"""
    if ak is None:
        return 0
    try:
        # 获取所有指数实时行情
        df = ak.stock_zh_index_spot_em()
        if df is not None and not df.empty:
            # 查找上证指数
            sh_index = df[df["名称"] == "上证指数"]
            if not sh_index.empty:
                return float(sh_index.iloc[0].get("涨跌幅", 0) or 0)
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
        
        # 自动保存报表到数据库
        try:
            today = date.today()
            save_report(today, market_change, sorted_result)
        except Exception as save_err:
            print(f"⚠️ 保存报表失败: {save_err}")
        
        return jsonify({
            "success": True,
            "data": sorted_result,
            "market_change": market_change
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== 报表存储与查询API ====================

@api.route('/api/reports')
def get_reports():
    """获取历史报表列表"""
    try:
        limit = request.args.get('limit', 30, type=int)
        reports = get_recent_reports(limit)
        return jsonify({
            "success": True,
            "data": reports
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@api.route('/api/reports/<report_date>')
def get_report(report_date):
    """获取指定日期的报表详情"""
    try:
        # 解析日期
        try:
            query_date = datetime.strptime(report_date, "%Y-%m-%d").date()
        except:
            return jsonify({"success": False, "error": "日期格式错误，请使用YYYY-MM-DD"}), 400
        
        report = get_report_by_date(query_date)
        if not report:
            return jsonify({"success": False, "error": "未找到该日期的报表"}), 404
        
        return jsonify({
            "success": True,
            "data": report
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== 收益跟踪API ====================

@api.route('/api/performance/summary')
def get_performance():
    """获取收益统计摘要"""
    try:
        days = request.args.get('days', 30, type=int)
        summary = get_performance_summary(days)
        return jsonify({
            "success": True,
            "data": summary
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@api.route('/api/performance/today')
def get_today_performance():
    """获取今日收益报告"""
    try:
        report = get_today_performance_report()
        return jsonify({
            "success": True,
            "data": report
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@api.route('/api/performance/update', methods=['POST'])
def trigger_performance_update():
    """手动触发收益更新"""
    try:
        update_all_performance()
        return jsonify({
            "success": True,
            "message": "收益更新完成"
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@api.route('/api/stock/<stock_code>/history')
def get_stock_recommend_history(stock_code):
    """获取股票的历史推荐记录"""
    try:
        limit = request.args.get('limit', 10, type=int)
        history = get_stock_history(stock_code, limit)
        return jsonify({
            "success": True,
            "data": history
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== 历史报表页面 ====================

@api.route('/history')
def history_page():
    """历史报表页面"""
    response = make_response(render_template('history.html'))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response


@api.route('/performance')
def performance_page():
    """收益统计页面"""
    response = make_response(render_template('performance.html'))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response


# ==================== K线数据API ====================

@api.route('/api/kline/<stock_code>')
def get_stock_kline(stock_code):
    """
    获取股票K线数据
    参数：
        days: 获取最近多少天的数据，默认250
    """
    import requests
    
    days = request.args.get('days', 250, type=int)
    
    try:
        # 判断市场（0=深圳 1=上海）
        market = "1" if stock_code.startswith(("6", "9")) else "0"
        
        # 东方财富K线接口
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": f"{market}.{stock_code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",  # 日K
            "fqt": "1",    # 前复权
            "end": "20500101",
            "lmt": str(days),
        }
        
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        
        if not data.get("data") or not data["data"].get("klines"):
            return jsonify({"success": False, "error": "无K线数据"}), 404
        
        stock_name = data["data"].get("name", "")
        klines = data["data"]["klines"]
        
        # 解析K线数据
        # 格式：日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
        result = []
        for kline in klines:
            parts = kline.split(",")
            if len(parts) >= 6:
                result.append({
                    "time": parts[0],           # 日期 YYYY-MM-DD
                    "open": float(parts[1]),    # 开盘价
                    "high": float(parts[3]),    # 最高价
                    "low": float(parts[4]),     # 最低价
                    "close": float(parts[2]),   # 收盘价
                    "volume": float(parts[5]),  # 成交量
                })
        
        return jsonify({
            "success": True,
            "data": {
                "code": stock_code,
                "name": stock_name,
                "klines": result
            }
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
