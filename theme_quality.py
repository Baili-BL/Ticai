# 题材质量评估模块 - 大、新、强
# 大(Big): 市场容量、想象空间
# 新(New): 新鲜度、未被充分炒作
# 强(Strong): 政策支持、产业趋势

from typing import Dict, List, Tuple

# 大题材关键词（万亿级赛道）
BIG_THEME_KEYWORDS = [
    "人工智能", "AI", "芯片", "半导体", "新能源", "光伏", "储能", "锂电",
    "汽车", "智能驾驶", "机器人", "数字经济", "云计算", "大数据", "5G", "6G",
    "医药", "创新药", "医疗器械", "军工", "航空航天", "卫星", "量子",
    "消费电子", "元宇宙", "虚拟现实", "AR", "VR", "物联网", "工业互联网",
]

# 强政策关键词（国家战略级）
STRONG_POLICY_KEYWORDS = [
    "国产替代", "自主可控", "信创", "安全", "数字中国", "新基建",
    "碳中和", "碳达峰", "双碳", "乡村振兴", "一带一路", "国企改革",
    "专精特新", "卡脖子", "核心技术", "战略新兴", "高端制造",
]

# 新兴概念关键词（近期热点）
NEW_CONCEPT_KEYWORDS = [
    "Sora", "GPT", "大模型", "AIGC", "生成式", "具身智能", "人形机器人",
    "低空经济", "飞行汽车", "eVTOL", "固态电池", "钠离子", "氢能",
    "脑机接口", "合成生物", "商业航天", "可控核聚变", "室温超导",
    "MR", "苹果", "华为", "鸿蒙", "星链", "算力", "液冷", "CPO",
]


def evaluate_theme_size(theme_name: str, theme_info: dict, stocks: List[dict]) -> Tuple[int, str]:
    """
    评估题材"大"的程度 (0-100)
    基于：市值容量、成分股数量、成交活跃度、关键词匹配
    """
    score = 0
    reasons = []
    
    # 1. 关键词匹配 (最高40分)
    keyword_match = 0
    for kw in BIG_THEME_KEYWORDS:
        if kw in theme_name:
            keyword_match += 15
            reasons.append(f"万亿赛道:{kw}")
            break
    score += min(keyword_match, 40)
    
    # 2. 成分股数量 (最高20分)
    up_count = theme_info.get("up_count", 0) or 0
    down_count = theme_info.get("down_count", 0) or 0
    total_stocks = up_count + down_count
    if total_stocks >= 100:
        score += 20
        reasons.append(f"成分股{total_stocks}只")
    elif total_stocks >= 50:
        score += 15
    elif total_stocks >= 30:
        score += 10
    elif total_stocks >= 15:
        score += 5
    
    # 3. 总市值规模 (最高25分)
    total_market_cap = sum(s.get("market_cap", 0) or 0 for s in stocks)
    if total_market_cap >= 1000000000000:  # 万亿
        score += 25
        reasons.append("万亿市值")
    elif total_market_cap >= 500000000000:  # 5000亿
        score += 20
    elif total_market_cap >= 100000000000:  # 1000亿
        score += 15
    elif total_market_cap >= 50000000000:  # 500亿
        score += 10
    
    # 4. 成交活跃度 (最高15分)
    total_amount = sum(s.get("amount", 0) or 0 for s in stocks)
    if total_amount >= 50000000000:  # 500亿成交
        score += 15
        reasons.append("成交活跃")
    elif total_amount >= 20000000000:  # 200亿
        score += 10
    elif total_amount >= 10000000000:  # 100亿
        score += 5
    
    return min(score, 100), "、".join(reasons[:2]) if reasons else ""


def evaluate_theme_novelty(theme_name: str, history: dict) -> Tuple[int, str]:
    """
    评估题材"新"的程度 (0-100)
    基于：新概念关键词、连续上涨天数（反向指标）、近期启动特征
    """
    score = 50  # 基础分
    reasons = []
    
    # 1. 新概念关键词匹配 (最高40分)
    for kw in NEW_CONCEPT_KEYWORDS:
        if kw in theme_name:
            score += 40
            reasons.append(f"新概念:{kw}")
            break
    
    # 2. 连续上涨天数（反向指标，涨太久说明不新鲜了）
    continuous_up = history.get("continuous_up", 0) or 0
    if continuous_up >= 5:
        score -= 30  # 连涨5天以上，已经不新鲜
        reasons.append("已连涨多日")
    elif continuous_up >= 3:
        score -= 15
    elif continuous_up <= 1:
        score += 15  # 刚启动，比较新鲜
        reasons.append("刚启动")
    
    # 3. 3日累计涨幅（反向指标）
    total_change_3d = history.get("total_change_3d", 0) or 0
    if total_change_3d >= 15:
        score -= 20  # 涨幅过大，可能已被充分炒作
    elif total_change_3d >= 10:
        score -= 10
    elif total_change_3d <= 3:
        score += 10  # 涨幅不大，还有空间
    
    return max(0, min(score, 100)), "、".join(reasons[:2]) if reasons else ""


def evaluate_theme_strength(theme_name: str, theme_info: dict, stocks: List[dict]) -> Tuple[int, str]:
    """
    评估题材"强"的程度 (0-100)
    基于：政策关键词、涨停数量、龙头强度、资金流入
    """
    score = 0
    reasons = []
    
    # 1. 政策关键词匹配 (最高35分)
    for kw in STRONG_POLICY_KEYWORDS:
        if kw in theme_name:
            score += 35
            reasons.append(f"政策支持:{kw}")
            break
    
    # 2. 涨停股数量 (最高25分)
    limit_up_count = sum(1 for s in stocks if (s.get("change_pct", 0) or 0) >= 9.9)
    if limit_up_count >= 5:
        score += 25
        reasons.append(f"{limit_up_count}只涨停")
    elif limit_up_count >= 3:
        score += 20
    elif limit_up_count >= 2:
        score += 15
    elif limit_up_count >= 1:
        score += 10
    
    # 3. 题材涨幅强度 (最高20分)
    change_pct = theme_info.get("change_pct", 0) or 0
    if change_pct >= 5:
        score += 20
        reasons.append("涨幅强劲")
    elif change_pct >= 3:
        score += 15
    elif change_pct >= 2:
        score += 10
    elif change_pct >= 1:
        score += 5
    
    # 4. 上涨家数占比 (最高20分)
    up_count = theme_info.get("up_count", 0) or 0
    down_count = theme_info.get("down_count", 0) or 0
    total = up_count + down_count
    if total > 0:
        up_ratio = up_count / total
        if up_ratio >= 0.8:
            score += 20
            reasons.append("普涨格局")
        elif up_ratio >= 0.7:
            score += 15
        elif up_ratio >= 0.6:
            score += 10
    
    return min(score, 100), "、".join(reasons[:2]) if reasons else ""


def evaluate_theme_quality(theme_name: str, theme_info: dict, stocks: List[dict], history: dict) -> dict:
    """
    综合评估题材质量，返回大、新、强评分
    """
    big_score, big_reason = evaluate_theme_size(theme_name, theme_info, stocks)
    new_score, new_reason = evaluate_theme_novelty(theme_name, history)
    strong_score, strong_reason = evaluate_theme_strength(theme_name, theme_info, stocks)
    
    # 综合评级
    total_score = (big_score + new_score + strong_score) / 3
    
    # 生成标签
    tags = []
    if big_score >= 60:
        tags.append({"name": "大", "score": big_score, "color": "#3498db", "desc": big_reason or "市场容量大"})
    if new_score >= 60:
        tags.append({"name": "新", "score": new_score, "color": "#9b59b6", "desc": new_reason or "概念新鲜"})
    if strong_score >= 60:
        tags.append({"name": "强", "score": strong_score, "color": "#e74c3c", "desc": strong_reason or "逻辑硬核"})
    
    # 综合评级文字
    if total_score >= 80:
        rating = "优质题材"
        rating_color = "#2ecc71"
    elif total_score >= 60:
        rating = "良好题材"
        rating_color = "#3498db"
    elif total_score >= 40:
        rating = "一般题材"
        rating_color = "#f39c12"
    else:
        rating = "弱势题材"
        rating_color = "#95a5a6"
    
    return {
        "big": {"score": big_score, "reason": big_reason},
        "new": {"score": new_score, "reason": new_reason},
        "strong": {"score": strong_score, "reason": strong_reason},
        "total_score": round(total_score, 1),
        "tags": tags,
        "rating": rating,
        "rating_color": rating_color,
    }
