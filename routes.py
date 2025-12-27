# 路由模块
from flask import Blueprint, jsonify, render_template
from theme_fetcher import fetch_hot_themes, fetch_all_themes_with_stocks
from analyzer import analyze_and_format_stocks

api = Blueprint('api', __name__)


@api.route('/')
def index():
    """首页"""
    return render_template('index.html')


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
        # 并发获取所有数据
        theme_data = fetch_all_themes_with_stocks(theme_limit=8)
        
        result = {}
        for theme_name, data in theme_data.items():
            stocks = data.get("stocks", [])
            theme_info = data.get("info", {})
            
            # 分析并格式化
            formatted_stocks = analyze_and_format_stocks(stocks)
            
            result[theme_name] = {
                "info": {
                    "change_pct": theme_info.get("change_pct", 0),
                    "up_count": theme_info.get("up_count", 0),
                    "down_count": theme_info.get("down_count", 0),
                },
                "stocks": formatted_stocks
            }
        
        # 按题材涨幅排序
        sorted_result = dict(sorted(
            result.items(), 
            key=lambda x: x[1]["info"].get("change_pct", 0), 
            reverse=True
        ))
        
        return jsonify({
            "success": True,
            "data": sorted_result
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500
