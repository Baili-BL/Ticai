# 题材获取模块 - 从东方财富获取实时热门题材
import requests
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import MAX_WORKERS, REQUEST_TIMEOUT, STOCKS_PER_THEME

# 缓存
_cache = {}
_cache_time = {}
CACHE_TTL = 300


def _get_cached(key):
    if key in _cache and time.time() - _cache_time.get(key, 0) < CACHE_TTL:
        return _cache[key]
    return None


def _set_cache(key, value):
    _cache[key] = value
    _cache_time[key] = time.time()


# 需要过滤的非题材标签（涨停形态、技术指标等）
EXCLUDE_KEYWORDS = [
    "连板", "一字板", "涨停", "跌停", "打板", "首板", "二板", "三板",
    "昨日", "今日", "反包", "炸板", "烂板", "换手板", "缩量板",
    "ST板块", "摘帽", "复牌", "新股", "次新", "破净", "破发",
    "高送转", "填权", "除权", "分红", "回购", "增持", "减持",
    "解禁", "质押", "融资融券", "北向资金", "主力", "游资",
    "龙虎榜", "大单", "资金流", "净流入", "净流出",
]


def is_valid_theme(name: str) -> bool:
    """判断是否为有效题材（排除涨停形态等标签）"""
    if not name:
        return False
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in name:
            return False
    return True


def fetch_hot_themes(limit=10) -> list:
    """获取热门题材板块列表"""
    cache_key = "hot_themes"
    cached = _get_cached(cache_key)
    if cached:
        return cached[:limit]

    try:
        # 东方财富概念板块接口
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 100,  # 多获取一些，过滤后保证数量
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",  # 按涨跌幅排序
            "fs": "m:90+t:3",  # 概念板块
            "fields": "f1,f2,f3,f4,f12,f13,f14,f104,f105,f128,f136,f152"
        }
        
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        
        if data.get("data") and data["data"].get("diff"):
            themes = []
            for item in data["data"]["diff"]:
                name = item.get("f14", "")
                # 过滤非题材标签
                if not is_valid_theme(name):
                    continue
                themes.append({
                    "code": item.get("f12", ""),
                    "name": name,
                    "change_pct": item.get("f3", 0),
                    "up_count": item.get("f104", 0),
                    "down_count": item.get("f105", 0),
                })
            _set_cache(cache_key, themes)
            return themes[:limit]
    except Exception as e:
        print(f"获取热门题材失败: {e}")
    
    return []


def fetch_theme_stocks(theme_code: str, theme_name: str) -> list:
    """获取单个题材的成分股"""
    cache_key = f"theme_stocks_{theme_code}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    try:
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 20,  # 获取前20只，后续筛选
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",  # 按涨跌幅排序
            "fs": f"b:{theme_code}",
            "fields": "f2,f3,f4,f5,f6,f7,f12,f14,f15,f16,f17,f18,f20,f21"
        }
        
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        
        stocks = []
        if data.get("data") and data["data"].get("diff"):
            for item in data["data"]["diff"]:
                code = item.get("f12", "")
                if not code:
                    continue
                stocks.append({
                    "code": code,
                    "name": item.get("f14", ""),
                    "price": item.get("f2", 0),
                    "change_pct": item.get("f3", 0),
                    "change_amt": item.get("f4", 0),
                    "volume": item.get("f5", 0),  # 成交量(手)
                    "amount": item.get("f6", 0),  # 成交额
                    "amplitude": item.get("f7", 0),  # 振幅
                    "high": item.get("f15", 0),
                    "low": item.get("f16", 0),
                    "open": item.get("f17", 0),
                    "prev_close": item.get("f18", 0),
                    "market_cap": item.get("f20", 0),  # 总市值
                    "float_cap": item.get("f21", 0),  # 流通市值
                    "theme": theme_name,
                })
        
        _set_cache(cache_key, stocks)
        return stocks
        
    except Exception as e:
        print(f"获取题材 {theme_name} 成分股失败: {e}")
        return []


def fetch_all_themes_with_stocks(theme_limit=8) -> dict:
    """并发获取所有热门题材及其股票"""
    # 1. 获取热门题材
    themes = fetch_hot_themes(theme_limit)
    if not themes:
        return {}
    
    # 2. 并发获取每个题材的成分股
    result = {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_theme = {
            executor.submit(fetch_theme_stocks, t["code"], t["name"]): t 
            for t in themes
        }
        
        for future in as_completed(future_to_theme):
            theme = future_to_theme[future]
            try:
                stocks = future.result()
                # 只取前 STOCKS_PER_THEME 只股票
                result[theme["name"]] = {
                    "info": theme,
                    "stocks": stocks[:STOCKS_PER_THEME]
                }
            except Exception as e:
                print(f"处理题材 {theme['name']} 失败: {e}")
    
    return result
