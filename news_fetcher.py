# 财经新闻抓取模块
# 支持多数据源：新浪财经、同花顺、东方财富

import time
import requests
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import akshare as ak
except ImportError:
    ak = None

# 请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

# 缓存
_news_cache = {}
_cache_time = {}
NEWS_CACHE_TTL = 180  # 3分钟缓存

# 重大利好关键词
POSITIVE_KEYWORDS = [
    # 政策利好
    "国务院", "发改委", "工信部", "央行", "证监会", "财政部",
    "政策支持", "扶持", "补贴", "减税", "降费", "利好",
    "战略", "规划", "意见", "通知", "指导",
    # 行业利好
    "突破", "首发", "首创", "领先", "第一", "龙头",
    "订单", "中标", "签约", "合作", "战略协议",
    "业绩预增", "净利润增长", "营收增长", "超预期",
    "涨价", "提价", "供不应求", "产能紧张",
    # 资金利好
    "增持", "回购", "举牌", "入股", "并购", "重组",
    "北向资金", "外资买入", "机构调研",
]

# 重大利空关键词
NEGATIVE_KEYWORDS = [
    "下跌", "暴跌", "大跌", "跳水", "崩盘",
    "减持", "清仓", "抛售", "套现",
    "亏损", "下滑", "下降", "业绩变脸",
    "处罚", "罚款", "违规", "调查", "立案",
    "退市", "ST", "*ST", "风险警示",
    "诉讼", "仲裁", "纠纷",
]

# 股票名称常见后缀（用于清洗）
STOCK_NAME_SUFFIXES = ["股份", "科技", "电子", "集团", "新材", "智能", "信息", "网络", "软件", "医药", "生物", "能源", "电气", "机械", "材料"]


def _get_cached(key):
    if key in _news_cache and time.time() - _cache_time.get(key, 0) < NEWS_CACHE_TTL:
        return _news_cache[key]
    return None


def _set_cache(key, value):
    _news_cache[key] = value
    _cache_time[key] = time.time()


# ============ 多源新闻获取 ============

def fetch_sina_news(limit: int = 30) -> List[Dict]:
    """新浪财经"""
    news_list = []
    try:
        url = 'https://feed.mix.sina.com.cn/api/roll/get'
        params = {'pageid': '153', 'lid': '2516', 'num': str(limit), 'page': '1'}
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('result', {}).get('data', [])[:limit]:
                news_list.append({
                    "title": item.get('title', ''),
                    "content": item.get('intro', '') or item.get('title', ''),
                    "time": item.get('ctime', ''),
                    "source": "新浪"
                })
    except Exception as e:
        print(f"新浪财经获取失败: {e}")
    return news_list


def fetch_ths_news(limit: int = 30) -> List[Dict]:
    """同花顺"""
    news_list = []
    try:
        url = 'https://news.10jqka.com.cn/tapp/news/push/stock/'
        params = {'page': '1', 'tag': '', 'track': 'website', 'pagesize': str(limit)}
        resp = requests.get(url, headers={**HEADERS, 'Referer': 'https://news.10jqka.com.cn/'}, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('data', {}).get('list', [])[:limit]:
                news_list.append({
                    "title": item.get('title', ''),
                    "content": item.get('digest', '') or item.get('title', ''),
                    "time": item.get('ctime', ''),
                    "source": "同花顺"
                })
    except Exception as e:
        print(f"同花顺获取失败: {e}")
    return news_list


def fetch_eastmoney_news(limit: int = 30) -> List[Dict]:
    """东方财富快讯"""
    news_list = []
    try:
        # 股票快讯
        url = 'https://np-anotice-stock.eastmoney.com/api/security/ann'
        params = {'sr': '-1', 'page_size': str(limit), 'page_index': '1', 'ann_type': 'A', 'client_source': 'web', 'f_node': '0'}
        resp = requests.get(url, headers={**HEADERS, 'Referer': 'https://data.eastmoney.com/'}, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get('data', {}).get('list', []) if data.get('data') else []
            for item in items[:limit]:
                title = item.get('NOTICETITLE', '')
                # 过滤掉纯公告类
                if title and not any(x in title for x in ['招股', '审计', '章程', '议案']):
                    news_list.append({
                        "title": title,
                        "content": title,
                        "time": item.get('NOTICETIME', ''),
                        "source": "东财"
                    })
    except Exception as e:
        print(f"东方财富获取失败: {e}")
    return news_list


def fetch_all_news(limit_per_source: int = 30) -> List[Dict]:
    """
    并发获取多源新闻
    """
    cache_key = f"all_news_{limit_per_source}"
    cached = _get_cached(cache_key)
    if cached:
        return cached
    
    all_news = []
    
    # 并发获取
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(fetch_sina_news, limit_per_source): "新浪",
            executor.submit(fetch_ths_news, limit_per_source): "同花顺",
            executor.submit(fetch_eastmoney_news, limit_per_source): "东财",
        }
        
        for future in as_completed(futures, timeout=15):
            source = futures[future]
            try:
                news = future.result()
                all_news.extend(news)
                print(f"  {source}: {len(news)}条")
            except Exception as e:
                print(f"  {source}获取失败: {e}")
    
    # 去重（按标题）
    seen_titles = set()
    unique_news = []
    for news in all_news:
        title = news.get('title', '')[:20]  # 取前20字作为去重key
        if title and title not in seen_titles:
            seen_titles.add(title)
            unique_news.append(news)
    
    if unique_news:
        _set_cache(cache_key, unique_news)
        print(f"📰 新闻聚合完成: 共{len(unique_news)}条 (去重后)")
    
    return unique_news


def fetch_cls_news(limit: int = 50) -> List[Dict]:
    """
    获取财经新闻（多源聚合）
    """
    return fetch_all_news(limit // 2)  # 每个源取一半


def analyze_news_sentiment(news_list: List[Dict]) -> Dict:
    """
    分析新闻整体情绪
    """
    if not news_list:
        return {"sentiment": "neutral", "score": 50, "positive_count": 0, "negative_count": 0}
    
    positive_count = 0
    negative_count = 0
    
    for news in news_list:
        content = news.get("content", "") + news.get("title", "")
        
        has_positive = any(kw in content for kw in POSITIVE_KEYWORDS)
        has_negative = any(kw in content for kw in NEGATIVE_KEYWORDS)
        
        if has_positive:
            positive_count += 1
        if has_negative:
            negative_count += 1
    
    # 计算情绪分数 (0-100)
    total = positive_count + negative_count
    if total == 0:
        score = 50
    else:
        score = int(50 + (positive_count - negative_count) / total * 50)
    score = max(0, min(100, score))
    
    if score >= 65:
        sentiment = "positive"
    elif score <= 35:
        sentiment = "negative"
    else:
        sentiment = "neutral"
    
    return {
        "sentiment": sentiment,
        "score": score,
        "positive_count": positive_count,
        "negative_count": negative_count,
    }


def extract_theme_keywords(theme_name: str, stocks: List[Dict] = None) -> List[str]:
    """
    动态提取题材关键词，无需手动维护映射表
    
    策略：
    1. 题材名称本身
    2. 智能拆分题材名（如"卫星互联网"→"卫星互联网"、"卫星"、"互联网"）
    3. 成分股名称（去除常见后缀）
    """
    keywords = []
    
    # 1. 题材名称本身（最高优先级）
    keywords.append(theme_name)
    
    # 2. 智能拆分题材名
    if len(theme_name) >= 4:
        common_roots = ["互联", "智能", "新能", "半导", "芯片", "机器", "卫星", "低空", "量子", "生物", "医药", "军工", "消费", "金融"]
        
        for i in range(2, len(theme_name) - 1):
            part1 = theme_name[:i]
            part2 = theme_name[i:]
            
            if len(part1) >= 2 and len(part2) >= 2:
                if part1 in common_roots or part2 in common_roots or len(part1) >= 3 or len(part2) >= 3:
                    keywords.append(part1)
                    keywords.append(part2)
    
    # 3. 从成分股名称提取关键词
    if stocks:
        for stock in stocks[:8]:
            stock_name = stock.get("name", "")
            if not stock_name or len(stock_name) < 2:
                continue
            
            clean_name = stock_name
            for suffix in STOCK_NAME_SUFFIXES:
                if clean_name.endswith(suffix):
                    clean_name = clean_name[:-len(suffix)]
                    break
            
            if len(clean_name) >= 2:
                keywords.append(clean_name)
            
            if len(stock_name) >= 3:
                keywords.append(stock_name)
    
    keywords = list(set(keywords))
    keywords.sort(key=len, reverse=True)
    
    return keywords


def find_theme_related_news(theme_name: str, news_list: List[Dict], stocks: List[Dict] = None) -> List[Dict]:
    """
    查找与特定题材相关的新闻
    使用动态提取的关键词进行匹配
    """
    related_news = []
    
    # 动态提取关键词
    keywords = extract_theme_keywords(theme_name, stocks)
    
    # 搜索新闻
    for news in news_list:
        content = news.get("content", "") + news.get("title", "")
        matched_keyword = None
        
        for kw in keywords:
            # 要求关键词长度至少3个字符，避免误匹配
            if len(kw) >= 3 and kw in content:
                matched_keyword = kw
                break
            # 对于2个字的关键词，要求是题材名本身
            elif len(kw) == 2 and kw == theme_name and kw in content:
                matched_keyword = kw
                break
        
        if matched_keyword:
            # 判断是利好还是利空
            is_positive = any(pk in content for pk in POSITIVE_KEYWORDS)
            is_negative = any(nk in content for nk in NEGATIVE_KEYWORDS)
            
            related_news.append({
                "content": content[:100],
                "time": news.get("time", 0),
                "is_positive": is_positive,
                "is_negative": is_negative,
                "matched_keyword": matched_keyword,
            })
    
    return related_news[:5]  # 最多返回5条


def evaluate_theme_news_factor(theme_name: str, news_list: List[Dict] = None, stocks: List[Dict] = None) -> Dict:
    """
    评估题材的消息面因子
    返回消息面评分和相关新闻
    
    参数:
        theme_name: 题材名称
        news_list: 新闻列表（可选，不传则自动获取）
        stocks: 题材内的股票列表（用于匹配新闻）
    """
    if news_list is None:
        news_list = fetch_cls_news(50)
    
    related_news = find_theme_related_news(theme_name, news_list, stocks)
    
    if not related_news:
        return {
            "score": 50,
            "level": "中性",
            "level_color": "#95a5a6",
            "news_count": 0,
            "positive_news": 0,
            "negative_news": 0,
            "latest_news": [],
            "summary": "暂无相关消息",
        }
    
    positive_news = sum(1 for n in related_news if n.get("is_positive"))
    negative_news = sum(1 for n in related_news if n.get("is_negative"))
    
    # 计算消息面分数
    score = 50
    if positive_news > negative_news:
        score = 50 + min((positive_news - negative_news) * 15, 40)
    elif negative_news > positive_news:
        score = 50 - min((negative_news - positive_news) * 15, 40)
    
    # 判断消息面级别
    if score >= 70:
        level = "利好"
        level_color = "#2ecc71"
    elif score <= 30:
        level = "利空"
        level_color = "#e74c3c"
    else:
        level = "中性"
        level_color = "#95a5a6"
    
    # 生成摘要
    if positive_news > 0 and negative_news == 0:
        summary = f"近期有{positive_news}条利好消息"
    elif negative_news > 0 and positive_news == 0:
        summary = f"近期有{negative_news}条利空消息"
    elif positive_news > 0 and negative_news > 0:
        summary = f"利好{positive_news}条，利空{negative_news}条"
    else:
        summary = f"有{len(related_news)}条相关消息"
    
    return {
        "score": score,
        "level": level,
        "level_color": level_color,
        "news_count": len(related_news),
        "positive_news": positive_news,
        "negative_news": negative_news,
        "latest_news": [n["content"] for n in related_news[:3]],
        "matched_keywords": list(set(n["matched_keyword"] for n in related_news)),
        "summary": summary,
    }


def get_market_news_summary() -> Dict:
    """
    获取市场整体消息面摘要
    """·
    news_list = fetch_cls_news(50)
    sentiment = analyze_news_sentiment(news_list)
    
    return {
        "total_news": len(news_list),
        "sentiment": sentiment["sentiment"],
        "sentiment_score": sentiment["score"],
        "positive_count": sentiment["positive_count"],
        "negative_count": sentiment["negative_count"],
        "important_positive": sentiment.get("important_positive", []),
        "important_negative": sentiment.get("important_negative", []),
    }
