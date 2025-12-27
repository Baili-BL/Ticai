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

# 需要过滤的非题材标签（涨停形态、技术指标等）
EXCLUDE_KEYWORDS = [
    "连板", "一字板", "涨停", "跌停", "打板", "首板", "二板", "三板",
    "昨日", "今日", "反包", "炸板", "烂板", "换手板", "缩量板",
    "ST板块", "摘帽", "复牌", "新股", "次新", "破净", "破发",
    "高送转", "填权", "除权", "分红", "回购", "增持", "减持",
    "解禁", "质押", "融资融券", "北向资金", "主力", "游资",
    "龙虎榜", "大单", "资金流", "净流入", "净流出",
]


def _get_cached(key):
    if key in _cache and time.time() - _cache_time.get(key, 0) < CACHE_TTL:
        return _cache[key]
    return None


def _set_cache(key, value):
    _cache[key] = value
    _cache_time[key] = time.time()


def is_valid_theme(name: str) -> bool:
    """判断是否为有效题材（排除涨停形态等标签）"""
    if not name:
        return False
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in name:
            return False
    return True


def fetch_theme_history(theme_code: str) -> dict:
    """
    获取题材近3日的历史数据（涨跌幅、资金流向）
    用于判断题材是否持续获得资金认可
    """
    cache_key = f"theme_history_{theme_code}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    result = {
        "days": [],
        "continuous_up": 0,      # 连续上涨天数
        "continuous_inflow": 0,  # 连续资金流入天数
        "total_change_3d": 0,    # 3日累计涨幅
        "total_inflow_3d": 0,    # 3日累计资金流入
        "is_hot": False,         # 是否为热门题材
    }

    try:
        # 获取板块K线数据（近5日）
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": f"90.{theme_code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",  # 日K
            "fqt": "1",
            "end": "20500101",
            "lmt": "5",
        }
        
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        
        if data.get("data") and data["data"].get("klines"):
            klines = data["data"]["klines"]
            days_data = []
            
            for kline in klines[-3:]:  # 取最近3天
                parts = kline.split(",")
                if len(parts) >= 9:
                    days_data.append({
                        "date": parts[0],
                        "close": float(parts[2]) if parts[2] else 0,
                        "change_pct": float(parts[8]) if parts[8] else 0,
                        "amount": float(parts[5]) if parts[5] else 0,
                    })
            
            result["days"] = days_data
            
            # 计算连续上涨天数
            continuous_up = 0
            for d in reversed(days_data):
                if d["change_pct"] > 0:
                    continuous_up += 1
                else:
                    break
            result["continuous_up"] = continuous_up
            
            # 计算3日累计涨幅
            if days_data:
                result["total_change_3d"] = sum(d["change_pct"] for d in days_data)
        
        # 获取资金流向数据
        flow_url = "http://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
        flow_params = {
            "secid": f"90.{theme_code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56",
            "klt": "101",
            "lmt": "5",
        }
        
        flow_resp = requests.get(flow_url, params=flow_params, timeout=REQUEST_TIMEOUT)
        flow_data = flow_resp.json()
        
        if flow_data.get("data") and flow_data["data"].get("klines"):
            flow_klines = flow_data["data"]["klines"]
            
            # 计算连续资金流入天数
            continuous_inflow = 0
            total_inflow = 0
            
            for kline in reversed(flow_klines[-3:]):
                parts = kline.split(",")
                if len(parts) >= 2:
                    # f52是主力净流入
                    inflow = float(parts[1]) if parts[1] else 0
                    total_inflow += inflow
                    if inflow > 0:
                        continuous_inflow += 1
                    else:
                        break
            
            result["continuous_inflow"] = continuous_inflow
            result["total_inflow_3d"] = total_inflow
        
        # 判断是否为热门题材（资金认可）
        # 条件：连续2天以上上涨 或 连续2天以上资金流入 或 3日累计涨幅>5%
        result["is_hot"] = (
            result["continuous_up"] >= 2 or 
            result["continuous_inflow"] >= 2 or 
            result["total_change_3d"] >= 5
        )
        
        _set_cache(cache_key, result)
        
    except Exception as e:
        print(f"获取题材历史数据失败 {theme_code}: {e}")
    
    return result


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
    themes = fetch_hot_themes(theme_limit + 5)  # 多获取一些，后续筛选
    if not themes:
        return {}
    
    result = {}
    
    # 2. 并发获取每个题材的成分股和历史数据
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交股票获取任务
        stock_futures = {
            executor.submit(fetch_theme_stocks, t["code"], t["name"]): t 
            for t in themes
        }
        # 提交历史数据获取任务
        history_futures = {
            executor.submit(fetch_theme_history, t["code"]): t 
            for t in themes
        }
        
        # 收集股票数据
        stock_results = {}
        for future in as_completed(stock_futures):
            theme = stock_futures[future]
            try:
                stocks = future.result()
                stock_results[theme["code"]] = stocks
            except Exception as e:
                print(f"获取股票失败 {theme['name']}: {e}")
                stock_results[theme["code"]] = []
        
        # 收集历史数据
        history_results = {}
        for future in as_completed(history_futures):
            theme = history_futures[future]
            try:
                history = future.result()
                history_results[theme["code"]] = history
            except Exception as e:
                print(f"获取历史失败 {theme['name']}: {e}")
                history_results[theme["code"]] = {}
    
    # 3. 组装结果，优先显示资金认可的题材
    theme_scores = []
    for t in themes:
        code = t["code"]
        history = history_results.get(code, {})
        
        # 计算题材热度分数
        score = 0
        if history.get("is_hot"):
            score += 50
        score += history.get("continuous_up", 0) * 15
        score += history.get("continuous_inflow", 0) * 15
        score += min(history.get("total_change_3d", 0) * 3, 30)
        score += t.get("change_pct", 0) * 2  # 今日涨幅
        
        theme_scores.append((t, score, history))
    
    # 按热度分数排序
    theme_scores.sort(key=lambda x: x[1], reverse=True)
    
    # 取前 theme_limit 个
    for t, score, history in theme_scores[:theme_limit]:
        code = t["code"]
        stocks = stock_results.get(code, [])
        
        result[t["name"]] = {
            "info": t,
            "stocks": stocks[:STOCKS_PER_THEME],
            "history": history,
            "hot_score": round(score, 1),
        }
    
    return result
