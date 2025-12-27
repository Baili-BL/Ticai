# 情绪周期分析模块
# 冰点 -> 启动 -> 发酵 -> 高潮 -> 分歧 -> 退潮 -> 再次冰点

from typing import Dict, List, Tuple


def calculate_theme_emotion(theme_info: dict, stocks: List[dict]) -> dict:
    """
    计算题材的情绪周期阶段
    
    量化指标：
    1. 题材涨跌幅
    2. 上涨家数占比
    3. 涨停股数量
    4. 平均成交额（活跃度）
    5. 平均振幅（分歧度）
    """
    change_pct = theme_info.get("change_pct", 0) or 0
    up_count = theme_info.get("up_count", 0) or 0
    down_count = theme_info.get("down_count", 0) or 0
    total_count = up_count + down_count
    
    # 上涨比例
    up_ratio = up_count / total_count * 100 if total_count > 0 else 50
    
    # 统计股票数据
    limit_up_count = 0  # 涨停数
    total_amount = 0
    total_amplitude = 0
    high_amplitude_count = 0  # 高振幅股票数（分歧指标）
    
    for s in stocks:
        chg = s.get("change_pct", 0) or 0
        if chg >= 9.9:
            limit_up_count += 1
        
        amount = s.get("amount", 0) or 0
        total_amount += amount
        
        amp = s.get("amplitude", 0) or 0
        total_amplitude += amp
        if amp > 8:  # 振幅超过8%视为高分歧
            high_amplitude_count += 1
    
    stock_count = len(stocks) if stocks else 1
    avg_amount = total_amount / stock_count
    avg_amplitude = total_amplitude / stock_count
    
    # 计算情绪分数 (0-100)
    emotion_score = calculate_emotion_score(
        change_pct=change_pct,
        up_ratio=up_ratio,
        limit_up_count=limit_up_count,
        avg_amount=avg_amount,
        avg_amplitude=avg_amplitude,
        high_amplitude_count=high_amplitude_count,
        stock_count=stock_count
    )
    
    # 判断周期阶段
    stage, stage_desc = determine_stage(
        emotion_score=emotion_score,
        change_pct=change_pct,
        up_ratio=up_ratio,
        avg_amplitude=avg_amplitude,
        limit_up_count=limit_up_count
    )
    
    return {
        "stage": stage,
        "stage_desc": stage_desc,
        "emotion_score": emotion_score,
        "metrics": {
            "change_pct": round(change_pct, 2),
            "up_ratio": round(up_ratio, 1),
            "limit_up_count": limit_up_count,
            "avg_amplitude": round(avg_amplitude, 2),
        }
    }


def calculate_emotion_score(
    change_pct: float,
    up_ratio: float,
    limit_up_count: int,
    avg_amount: float,
    avg_amplitude: float,
    high_amplitude_count: int,
    stock_count: int
) -> int:
    """
    计算情绪综合分数 (0-100)
    """
    score = 50  # 基准分
    
    # 1. 涨跌幅贡献 (-20 ~ +25)
    if change_pct >= 5:
        score += 25
    elif change_pct >= 3:
        score += 20
    elif change_pct >= 1:
        score += 10
    elif change_pct >= 0:
        score += 5
    elif change_pct >= -1:
        score -= 5
    elif change_pct >= -3:
        score -= 15
    else:
        score -= 20
    
    # 2. 上涨比例贡献 (-15 ~ +15)
    if up_ratio >= 80:
        score += 15
    elif up_ratio >= 60:
        score += 10
    elif up_ratio >= 50:
        score += 5
    elif up_ratio >= 40:
        score -= 5
    elif up_ratio >= 30:
        score -= 10
    else:
        score -= 15
    
    # 3. 涨停数贡献 (0 ~ +15)
    if limit_up_count >= 5:
        score += 15
    elif limit_up_count >= 3:
        score += 10
    elif limit_up_count >= 1:
        score += 5
    
    # 4. 成交活跃度 (0 ~ +10)
    if avg_amount >= 1500000000:  # 15亿
        score += 10
    elif avg_amount >= 800000000:  # 8亿
        score += 6
    elif avg_amount >= 300000000:  # 3亿
        score += 3
    
    # 5. 分歧度调整（高振幅可能是分歧信号）
    if avg_amplitude > 10 and change_pct < 3:
        score -= 10  # 高振幅但涨幅不大，分歧明显
    
    return max(0, min(100, score))


def determine_stage(
    emotion_score: int,
    change_pct: float,
    up_ratio: float,
    avg_amplitude: float,
    limit_up_count: int
) -> Tuple[str, str]:
    """
    判断情绪周期阶段
    
    返回: (阶段名称, 阶段描述)
    """
    
    # 高潮特征：高情绪分 + 高涨幅 + 多涨停
    if emotion_score >= 80 and change_pct >= 4 and limit_up_count >= 3:
        return "高潮", "情绪亢奋，注意风险"
    
    # 发酵特征：较高情绪 + 上涨 + 有涨停
    if emotion_score >= 65 and change_pct >= 2:
        return "发酵", "资金持续流入"
    
    # 启动特征：情绪回暖 + 小幅上涨
    if 55 <= emotion_score < 70 and change_pct >= 0.5:
        return "启动", "关注龙头表现"
    
    # 分歧特征：中等情绪 + 高振幅 + 涨跌参半
    if 40 <= emotion_score < 65 and avg_amplitude >= 6 and 35 <= up_ratio <= 65:
        return "分歧", "多空博弈激烈"
    
    # 退潮特征：情绪下降 + 下跌
    if 25 <= emotion_score < 50 and change_pct < 0:
        return "退潮", "资金撤离中"
    
    # 冰点特征：低情绪 + 明显下跌
    if emotion_score < 30 and change_pct < -2:
        return "冰点", "等待企稳信号"
    
    # 再次冰点（深度冰点）
    if emotion_score < 20:
        return "冰点", "极度低迷"
    
    # 默认：震荡观望
    if change_pct >= 0:
        return "震荡", "方向不明"
    else:
        return "调整", "暂时观望"


def get_stage_color(stage: str) -> str:
    """获取阶段对应的颜色"""
    colors = {
        "冰点": "#6c757d",   # 灰色
        "启动": "#17a2b8",   # 青色
        "发酵": "#28a745",   # 绿色
        "高潮": "#dc3545",   # 红色
        "分歧": "#ffc107",   # 黄色
        "退潮": "#fd7e14",   # 橙色
        "震荡": "#6c757d",   # 灰色
        "调整": "#6c757d",   # 灰色
    }
    return colors.get(stage, "#6c757d")


def get_stage_advice(stage: str) -> str:
    """获取阶段操作建议"""
    advice = {
        "冰点": "耐心等待，不宜抄底",
        "启动": "轻仓试探，关注龙头",
        "发酵": "可适当参与，跟随龙头",
        "高潮": "谨慎追高，注意止盈",
        "分歧": "观察为主，等待方向",
        "退潮": "规避风险，不宜介入",
        "震荡": "观望等待，方向不明",
        "调整": "暂时回避，等待企稳",
    }
    return advice.get(stage, "观望")
