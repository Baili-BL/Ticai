# 股票分析模块 - 基于短线实战体系
from typing import List, Dict


def analyze_volume_price(stock: dict) -> dict:
    """
    量价关系分析 - 核心中的核心
    判断：放量上涨、缩量上涨、放量滞涨等
    """
    volume = stock.get("volume", 0) or 0
    amount = stock.get("amount", 0) or 0
    change_pct = stock.get("change_pct", 0) or 0
    amplitude = stock.get("amplitude", 0) or 0
    
    # 计算量能等级
    volume_level = "低"
    if amount > 2000000000:  # 20亿以上
        volume_level = "爆量"
    elif amount > 1000000000:  # 10亿以上
        volume_level = "放量"
    elif amount > 500000000:  # 5亿以上
        volume_level = "中量"
    
    # 量价配合判断
    signal = "观望"
    if volume_level in ["爆量", "放量"] and change_pct > 3:
        signal = "放量上涨"  # 主力介入信号
    elif volume_level == "中量" and change_pct > 5:
        signal = "缩量强势"  # 筹码锁定良好
    elif volume_level in ["爆量", "放量"] and -1 < change_pct < 2:
        signal = "放量滞涨"  # 警惕出货
    elif volume_level in ["爆量", "放量"] and change_pct < -3:
        signal = "放量下跌"  # 主力出逃
    elif change_pct > 2:
        signal = "温和上涨"
    
    return {
        "volume_level": volume_level,
        "signal": signal,
        "is_healthy": signal in ["放量上涨", "缩量强势", "温和上涨"]
    }


def analyze_position(stock: dict) -> dict:
    """
    位置分析 - 判断股价所处位置
    基于振幅、涨跌幅判断是否处于启动位置
    """
    change_pct = stock.get("change_pct", 0) or 0
    amplitude = stock.get("amplitude", 0) or 0
    high = stock.get("high", 0) or 0
    low = stock.get("low", 0) or 0
    price = stock.get("price", 0) or 0
    prev_close = stock.get("prev_close", 0) or 0
    
    position = "中位"
    
    if high > 0 and low > 0 and price > 0:
        # 计算当前价格在日内的位置
        day_range = high - low
        if day_range > 0:
            price_position = (price - low) / day_range
            if price_position > 0.8:
                position = "日内高位"
            elif price_position < 0.3:
                position = "日内低位"
    
    # 涨停判断
    is_limit_up = change_pct >= 9.9
    is_near_limit = 7 <= change_pct < 9.9
    
    return {
        "position": position,
        "is_limit_up": is_limit_up,
        "is_near_limit": is_near_limit,
        "amplitude": amplitude
    }


def analyze_strength(stock: dict) -> dict:
    """
    强度分析 - 弱转强模式识别
    """
    change_pct = stock.get("change_pct", 0) or 0
    amplitude = stock.get("amplitude", 0) or 0
    open_price = stock.get("open", 0) or 0
    prev_close = stock.get("prev_close", 0) or 0
    price = stock.get("price", 0) or 0
    
    # 开盘强度
    open_strength = "平开"
    if prev_close > 0:
        open_change = (open_price - prev_close) / prev_close * 100
        if open_change > 3:
            open_strength = "高开"
        elif open_change < -2:
            open_strength = "低开"
    
    # 弱转强判断：低开高走或盘中回踩后拉升
    is_weak_to_strong = False
    if open_strength == "低开" and change_pct > 3:
        is_weak_to_strong = True
    elif amplitude > 5 and change_pct > 3:
        is_weak_to_strong = True
    
    # 整体强度评级
    strength = "弱"
    if change_pct >= 9.9:
        strength = "涨停"
    elif change_pct >= 7:
        strength = "强势"
    elif change_pct >= 3:
        strength = "偏强"
    elif change_pct >= 0:
        strength = "震荡"
    elif change_pct >= -3:
        strength = "偏弱"
    else:
        strength = "弱势"
    
    return {
        "open_strength": open_strength,
        "strength": strength,
        "is_weak_to_strong": is_weak_to_strong
    }


def calculate_score(stock: dict) -> tuple:
    """
    综合评分 - 基于短线实战体系
    返回: (分数, 分析详情)
    """
    if not stock or stock.get("price", 0) == 0:
        return 0, {}
    
    score = 40  # 基础分
    details = {}
    
    # 1. 量价分析 (权重最高 - 30分)
    vp = analyze_volume_price(stock)
    details["volume_price"] = vp
    if vp["signal"] == "放量上涨":
        score += 30
    elif vp["signal"] == "缩量强势":
        score += 25
    elif vp["signal"] == "温和上涨":
        score += 15
    elif vp["signal"] == "放量滞涨":
        score -= 10
    elif vp["signal"] == "放量下跌":
        score -= 20
    
    # 2. 强度分析 (20分)
    strength = analyze_strength(stock)
    details["strength"] = strength
    if strength["strength"] == "涨停":
        score += 20
    elif strength["strength"] == "强势":
        score += 18
    elif strength["strength"] == "偏强":
        score += 12
    elif strength["strength"] == "震荡":
        score += 5
    elif strength["strength"] in ["偏弱", "弱势"]:
        score -= 5
    
    # 弱转强加分
    if strength["is_weak_to_strong"]:
        score += 10
        details["weak_to_strong"] = True
    
    # 3. 位置分析 (10分)
    pos = analyze_position(stock)
    details["position"] = pos
    if pos["position"] == "日内高位" and not pos["is_limit_up"]:
        score -= 5  # 追高风险
    elif pos["is_near_limit"]:
        score += 8  # 冲板预期
    
    # 4. 市值因素 (流动性考量)
    market_cap = stock.get("market_cap", 0) or 0
    if 5000000000 < market_cap < 50000000000:  # 50-500亿，中等市值
        score += 5
    elif market_cap > 100000000000:  # 千亿以上大盘股
        score += 2
    
    return max(0, min(100, round(score))), details


def get_trading_signal(stock: dict, details: dict) -> str:
    """生成交易信号建议"""
    signals = []
    
    vp = details.get("volume_price", {})
    strength = details.get("strength", {})
    pos = details.get("position", {})
    
    # 量价信号
    if vp.get("signal") == "放量上涨":
        signals.append("量价齐升")
    elif vp.get("signal") == "缩量强势":
        signals.append("筹码锁定")
    elif vp.get("signal") == "放量滞涨":
        signals.append("⚠️滞涨")
    elif vp.get("signal") == "放量下跌":
        signals.append("⚠️出货")
    
    # 强度信号
    if strength.get("is_weak_to_strong"):
        signals.append("弱转强✓")
    if strength.get("strength") == "涨停":
        signals.append("涨停封板")
    elif strength.get("strength") == "强势":
        signals.append("强势领涨")
    
    # 位置信号
    if pos.get("is_near_limit"):
        signals.append("冲板中")
    
    return " | ".join(signals) if signals else "观望"


def get_recommendation_reason(stock: dict, details: dict) -> str:
    """生成推荐理由"""
    reasons = []
    
    vp = details.get("volume_price", {})
    strength = details.get("strength", {})
    
    # 量能描述
    vol_level = vp.get("volume_level", "")
    if vol_level in ["爆量", "放量"]:
        reasons.append(f"{vol_level}活跃")
    
    # 强度描述
    s = strength.get("strength", "")
    if s == "涨停":
        reasons.append("封板强势")
    elif s == "强势":
        reasons.append("领涨题材")
    elif s == "偏强":
        reasons.append("稳步上攻")
    
    # 特殊形态
    if details.get("weak_to_strong"):
        reasons.append("弱转强形态")
    
    open_s = strength.get("open_strength", "")
    if open_s == "高开":
        reasons.append("高开强势")
    elif open_s == "低开" and strength.get("strength") in ["强势", "偏强"]:
        reasons.append("低开高走")
    
    return "，".join(reasons) if reasons else "综合表现一般"


def format_amount(amount):
    """格式化成交额"""
    if not amount:
        return "-"
    amount = float(amount)
    if amount >= 100000000:
        return f"{amount/100000000:.2f}亿"
    elif amount >= 10000:
        return f"{amount/10000:.0f}万"
    return str(amount)


def format_market_cap(cap):
    """格式化市值"""
    if not cap:
        return "-"
    cap = float(cap)
    if cap >= 100000000:
        return f"{cap/100000000:.0f}亿"
    return "-"


def format_stock_display(stock: dict) -> dict:
    """格式化股票显示数据"""
    if not stock:
        return {"error": "无数据"}
    
    price = stock.get("price", 0)
    if not price or price == "-":
        return {
            "code": stock.get("code", ""),
            "name": stock.get("name", ""),
            "error": "停牌或无数据",
        }
    
    change_pct = stock.get("change_pct", 0) or 0
    score, details = calculate_score(stock)
    
    result = {
        "code": stock.get("code", ""),
        "name": stock.get("name", ""),
        "price": f"{float(price):.2f}" if price else "-",
        "change_pct": f"{change_pct:+.2f}%",
        "change_pct_num": change_pct,
        "volume": f"{(stock.get('volume', 0) or 0)/10000:.1f}万手",
        "amount": format_amount(stock.get("amount", 0)),
        "market_cap": format_market_cap(stock.get("market_cap", 0)),
        "amplitude": f"{stock.get('amplitude', 0) or 0:.2f}%",
        "score": score,
        "signal": get_trading_signal(stock, details),
        "reason": get_recommendation_reason(stock, details),
        # 详细分析数据
        "volume_level": details.get("volume_price", {}).get("volume_level", "-"),
        "strength": details.get("strength", {}).get("strength", "-"),
        "is_weak_to_strong": details.get("weak_to_strong", False),
    }
    
    return result


def analyze_and_format_stocks(stocks: List[dict]) -> List[dict]:
    """分析并格式化股票列表，按评分排序"""
    formatted = [format_stock_display(s) for s in stocks]
    # 过滤掉有错误的，按评分排序
    valid = [f for f in formatted if "error" not in f]
    invalid = [f for f in formatted if "error" in f]
    valid.sort(key=lambda x: x.get("score", 0), reverse=True)
    return valid + invalid
