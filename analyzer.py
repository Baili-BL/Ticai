# 股票分析模块 - 基于短线实战体系
from typing import List, Dict


def analyze_volume_price(stock: dict) -> dict:
    """
    量价关系分析 - 核心中的核心
    判断：放量上涨、缩量上涨、放量滞涨等
    
    量能判断改用换手率（成交额/流通市值）更合理
    """
    amount = stock.get("amount", 0) or 0
    change_pct = stock.get("change_pct", 0) or 0
    float_cap = stock.get("float_cap", 0) or 0  # 流通市值
    market_cap = stock.get("market_cap", 0) or 0  # 总市值
    
    # 计算换手率（用成交额/流通市值估算）
    # 如果没有流通市值，用总市值的70%估算
    cap = float_cap if float_cap > 0 else (market_cap * 0.7 if market_cap > 0 else 0)
    
    turnover_rate = 0
    if cap > 0:
        turnover_rate = (amount / cap) * 100  # 换手率百分比
    
    # 量能等级（基于换手率）
    volume_level = "低"
    if turnover_rate >= 15:
        volume_level = "爆量"  # 换手率>=15%
    elif turnover_rate >= 8:
        volume_level = "放量"  # 换手率>=8%
    elif turnover_rate >= 4:
        volume_level = "中量"  # 换手率>=4%
    elif turnover_rate >= 2:
        volume_level = "缩量"  # 换手率>=2%
    else:
        volume_level = "地量"  # 换手率<2%
    
    # 量价配合判断
    signal = "观望"
    if volume_level in ["爆量", "放量"] and change_pct > 3:
        signal = "放量上涨"  # 主力介入信号
    elif volume_level in ["中量", "缩量"] and change_pct > 5:
        signal = "缩量强势"  # 筹码锁定良好
    elif volume_level in ["爆量", "放量"] and -1 < change_pct < 2:
        signal = "放量滞涨"  # 警惕出货
    elif volume_level in ["爆量", "放量"] and change_pct < -3:
        signal = "放量下跌"  # 主力出逃
    elif change_pct > 2:
        signal = "温和上涨"
    elif volume_level == "地量" and change_pct > 0:
        signal = "无量上涨"  # 可能是一字板或缩量惜售
    
    return {
        "volume_level": volume_level,
        "turnover_rate": round(turnover_rate, 2),  # 换手率
        "signal": signal,
        "is_healthy": signal in ["放量上涨", "缩量强势", "温和上涨", "无量上涨"]
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


def analyze_strength(stock: dict, market_change: float = 0, theme_change: float = 0) -> dict:
    """
    强度分析 - 弱转强模式识别 + 前排强度判断
    
    前排强度核心：
    1. 涨速快 - 短时间内涨幅大
    2. 逆势强 - 大盘跌它涨，板块弱它强
    3. 领涨力 - 板块内涨幅领先
    
    参数:
        stock: 股票数据
        market_change: 大盘涨跌幅（用于判断逆势）
        theme_change: 板块涨跌幅（用于判断板块内强度）
    """
    change_pct = stock.get("change_pct", 0) or 0
    amplitude = stock.get("amplitude", 0) or 0
    open_price = stock.get("open", 0) or 0
    prev_close = stock.get("prev_close", 0) or 0
    price = stock.get("price", 0) or 0
    high = stock.get("high", 0) or 0
    low = stock.get("low", 0) or 0
    
    # 开盘强度（竞价强度）
    open_strength = "平开"
    open_change = 0
    if prev_close > 0 and open_price > 0:
        open_change = (open_price - prev_close) / prev_close * 100
        if open_change >= 9.5:
            open_strength = "竞价涨停"
        elif open_change >= 5:
            open_strength = "竞价强势"
        elif open_change >= 3:
            open_strength = "高开强势"
        elif open_change >= 1:
            open_strength = "小幅高开"
        elif open_change <= -3:
            open_strength = "低开弱势"
        elif open_change <= -1:
            open_strength = "小幅低开"
    
    # 弱转强判断
    is_weak_to_strong = False
    weak_to_strong_type = ""
    
    if open_change <= 0 and change_pct >= 3:
        is_weak_to_strong = True
        weak_to_strong_type = "低开高走"
    
    if amplitude >= 5 and change_pct >= 3:
        if high > 0 and low > 0 and price > 0:
            day_range = high - low
            if day_range > 0:
                price_pos = (price - low) / day_range
                if price_pos >= 0.7:
                    is_weak_to_strong = True
                    weak_to_strong_type = "回踩确认"
    
    if amplitude >= 4 and change_pct >= 5 and open_change <= 1:
        is_weak_to_strong = True
        weak_to_strong_type = "分时反转"
    
    # ========== 前排强度判断 ==========
    is_front_runner = False
    front_runner_tags = []
    
    # 1. 逆势强度 - 大盘跌它涨
    if market_change < -0.5 and change_pct > 0:
        # 大盘跌超0.5%，它还涨
        is_front_runner = True
        front_runner_tags.append("逆势上涨")
    elif market_change < 0 and change_pct >= 3:
        # 大盘跌，它涨3%以上
        is_front_runner = True
        front_runner_tags.append("逆势走强")
    
    # 2. 板块内强度 - 板块弱它强
    if theme_change < 1 and change_pct >= theme_change + 3:
        # 比板块强3%以上
        is_front_runner = True
        front_runner_tags.append("板块领涨")
    elif theme_change < 0 and change_pct > 0:
        # 板块跌它涨
        is_front_runner = True
        front_runner_tags.append("独立走强")
    
    # 3. 涨速强度 - 振幅大且收高位（说明拉升快）
    if amplitude >= 6 and change_pct >= 5:
        if high > 0 and low > 0 and price > 0:
            day_range = high - low
            if day_range > 0:
                price_pos = (price - low) / day_range
                if price_pos >= 0.8:  # 收在日内最高位附近
                    is_front_runner = True
                    front_runner_tags.append("涨速凌厉")
    
    # 4. 封板强度 - 涨停且振幅小（说明封板早、封得死）
    if change_pct >= 9.9:
        if amplitude <= 5:
            is_front_runner = True
            front_runner_tags.append("强势封板")
        elif amplitude <= 8:
            front_runner_tags.append("封板")
    
    # 5. 竞价强度转化 - 竞价高开且维持强势
    if open_change >= 3 and change_pct >= open_change:
        is_front_runner = True
        front_runner_tags.append("竞价兑现")
    
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
        "open_change": round(open_change, 2),
        "strength": strength,
        "is_weak_to_strong": is_weak_to_strong,
        "weak_to_strong_type": weak_to_strong_type,
        # 前排强度
        "is_front_runner": is_front_runner,
        "front_runner_tags": front_runner_tags,
    }


def calculate_score(stock: dict, market_change: float = 0, theme_change: float = 0) -> tuple:
    """
    综合评分 - 基于短线实战体系
    返回: (分数, 分析详情)
    
    参数:
        stock: 股票数据
        market_change: 大盘涨跌幅
        theme_change: 板块涨跌幅
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
    
    # 2. 强度分析 (20分) - 传入市场和板块数据
    strength = analyze_strength(stock, market_change, theme_change)
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
    
    # 前排强度加分
    if strength["is_front_runner"]:
        score += 8
        details["is_front_runner"] = True
        details["front_runner_tags"] = strength["front_runner_tags"]
    
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


def format_stock_display(stock: dict, theme_stocks: List[dict] = None, market_change: float = 0, theme_change: float = 0) -> dict:
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
    score, details = calculate_score(stock, market_change, theme_change)
    strength_info = details.get("strength", {})
    
    # 判断是否率先涨停
    is_first_limit = False
    if change_pct >= 9.9 and theme_stocks:
        limit_stocks = [s for s in theme_stocks if (s.get("change_pct", 0) or 0) >= 9.9]
        if limit_stocks and stock.get("code") == limit_stocks[0].get("code"):
            is_first_limit = True
    
    # 竞价强度标签
    open_strength = strength_info.get("open_strength", "平开")
    open_change = strength_info.get("open_change", 0)
    
    # 前排强度
    is_front_runner = strength_info.get("is_front_runner", False)
    front_runner_tags = strength_info.get("front_runner_tags", [])
    
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
        "turnover_rate": f"{details.get('volume_price', {}).get('turnover_rate', 0):.1f}%",
        "score": score,
        "signal": get_trading_signal(stock, details),
        "reason": get_recommendation_reason(stock, details),
        # 详细分析数据
        "volume_level": details.get("volume_price", {}).get("volume_level", "-"),
        "strength": strength_info.get("strength", "-"),
        "is_weak_to_strong": strength_info.get("is_weak_to_strong", False),
        "weak_to_strong_type": strength_info.get("weak_to_strong_type", ""),
        # 特殊标签
        "is_first_limit": is_first_limit,
        "open_strength": open_strength,
        "open_change": open_change,
        "is_limit_up": change_pct >= 9.9,
        # 前排强度
        "is_front_runner": is_front_runner,
        "front_runner_tags": front_runner_tags,
    }
    
    return result


def analyze_and_format_stocks(stocks: List[dict], market_change: float = 0, theme_change: float = 0) -> List[dict]:
    """
    分析并格式化股票列表，按评分排序
    
    参数:
        stocks: 股票列表
        market_change: 大盘涨跌幅（用于判断逆势）
        theme_change: 板块涨跌幅（用于判断板块内强度）
    """
    formatted = [format_stock_display(s, stocks, market_change, theme_change) for s in stocks]
    # 过滤掉有错误的，按评分排序
    valid = [f for f in formatted if "error" not in f]
    invalid = [f for f in formatted if "error" in f]
    valid.sort(key=lambda x: x.get("score", 0), reverse=True)
    
    # 标记率先涨停（排序后第一个涨停的）
    found_first_limit = False
    for s in valid:
        if s.get("is_limit_up") and not found_first_limit:
            s["is_first_limit"] = True
            found_first_limit = True
        elif s.get("is_first_limit"):
            s["is_first_limit"] = False
    
    return valid + invalid
