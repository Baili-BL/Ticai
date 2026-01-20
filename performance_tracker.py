# 收益跟踪模块 - 每日更新推荐股票的实盘收益
import requests
from datetime import datetime, date, timedelta
from typing import List, Dict
from database import (
    get_stocks_for_tracking, 
    save_performance, 
    get_connection
)
from config import REQUEST_TIMEOUT

# 缓存当天的股票价格
_price_cache = {}


def get_current_price(stock_code: str) -> float:
    """
    获取股票当前价格
    使用东方财富实时行情接口
    """
    global _price_cache
    
    # 检查缓存
    cache_key = f"{stock_code}_{date.today()}"
    if cache_key in _price_cache:
        return _price_cache[cache_key]
    
    try:
        # 判断市场（0=深圳 1=上海）
        market = "1" if stock_code.startswith(("6", "9")) else "0"
        
        url = "http://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": f"{market}.{stock_code}",
            "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f169,f170"
        }
        
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        
        if data.get("data"):
            # f43 是最新价（单位：分，需要除以100）
            price = data["data"].get("f43", 0)
            if price and price != "-":
                price = float(price) / 100
                _price_cache[cache_key] = price
                return price
    except Exception as e:
        print(f"获取股票 {stock_code} 价格失败: {e}")
    
    return 0


def get_batch_prices(stock_codes: List[str]) -> Dict[str, float]:
    """
    批量获取股票价格（更高效）
    """
    if not stock_codes:
        return {}
    
    prices = {}
    
    try:
        # 构建secids参数
        secids = []
        for code in stock_codes:
            market = "1" if code.startswith(("6", "9")) else "0"
            secids.append(f"{market}.{code}")
        
        # 东方财富批量接口
        url = "http://push2.eastmoney.com/api/qt/ulist/get"
        params = {
            "fltt": "2",
            "secids": ",".join(secids),
            "fields": "f2,f12"  # f2=最新价, f12=代码
        }
        
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()
        
        if data.get("data") and data["data"].get("diff"):
            for item in data["data"]["diff"]:
                code = item.get("f12", "")
                price = item.get("f2", 0)
                if code and price and price != "-":
                    prices[code] = float(price)
                    
    except Exception as e:
        print(f"批量获取价格失败: {e}")
        # 失败时逐个获取
        for code in stock_codes:
            prices[code] = get_current_price(code)
    
    return prices


def calculate_return(recommend_price: float, current_price: float) -> float:
    """计算收益率"""
    if recommend_price <= 0 or current_price <= 0:
        return 0
    return round((current_price - recommend_price) / recommend_price * 100, 2)


def is_trading_day(check_date: date = None) -> bool:
    """
    判断是否为交易日
    简单判断：周一到周五为交易日（不考虑节假日）
    """
    if check_date is None:
        check_date = date.today()
    return check_date.weekday() < 5


def get_trading_days_between(start_date: date, end_date: date) -> int:
    """计算两个日期之间的交易日数量"""
    if end_date <= start_date:
        return 0
    
    trading_days = 0
    current = start_date + timedelta(days=1)
    while current <= end_date:
        if is_trading_day(current):
            trading_days += 1
        current += timedelta(days=1)
    
    return trading_days


def update_all_performance():
    """
    更新所有需要跟踪的股票收益
    每日运行一次，更新最近5天推荐股票的收益
    """
    print("\n" + "=" * 60)
    print("📊 开始更新收益跟踪...")
    print("=" * 60)
    
    today = date.today()
    
    # 检查是否为交易日
    if not is_trading_day(today):
        print("⚠️ 今天不是交易日，跳过更新")
        return
    
    # 获取需要跟踪的股票（最近5天推荐的）
    stocks = get_stocks_for_tracking(days_ago=5)
    
    if not stocks:
        print("ℹ️ 没有需要跟踪的股票")
        return
    
    print(f"📋 需要更新 {len(stocks)} 只股票的收益")
    
    # 批量获取当前价格
    stock_codes = list(set(s['stock_code'] for s in stocks))
    prices = get_batch_prices(stock_codes)
    
    # 更新每只股票的收益
    updated_count = 0
    for stock in stocks:
        stock_code = stock['stock_code']
        stock_id = stock['id']
        recommend_price = stock['recommend_price']
        report_date_str = stock['report_date']
        
        # 解析推荐日期
        if isinstance(report_date_str, str):
            report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
        else:
            report_date = report_date_str
        
        # 计算持有天数（交易日）
        days_held = get_trading_days_between(report_date, today)
        
        if days_held < 1:
            continue  # 推荐当天不计算
        
        # 获取当前价格
        current_price = prices.get(stock_code, 0)
        if current_price <= 0:
            current_price = get_current_price(stock_code)
        
        if current_price <= 0:
            print(f"  ⚠️ {stock['stock_name']}({stock_code}) 获取价格失败")
            continue
        
        # 计算收益率
        return_pct = calculate_return(recommend_price, current_price)
        
        # 保存收益记录
        save_performance(
            stock_id=stock_id,
            track_date=today,
            days_held=days_held,
            current_price=current_price,
            return_pct=return_pct,
            is_trading_day=True
        )
        
        updated_count += 1
        
        # 打印日志
        emoji = "🔴" if return_pct < 0 else "🟢"
        print(f"  {emoji} {stock['stock_name']}({stock_code}) T+{days_held}: "
              f"{recommend_price:.2f} → {current_price:.2f} ({return_pct:+.2f}%)")
    
    print(f"\n✅ 收益更新完成，共更新 {updated_count} 条记录")
    print("=" * 60 + "\n")


def get_today_performance_report() -> Dict:
    """
    生成今日收益报告
    返回各时间段的收益统计
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    today = date.today()
    
    report = {
        "date": str(today),
        "summary": {},
        "details": []
    }
    
    # 获取今日更新的收益记录
    cursor.execute('''
        SELECT p.*, rs.stock_code, rs.stock_name, rs.theme_name, 
               rs.role, rs.recommend_price, r.report_date
        FROM performance p
        JOIN recommended_stocks rs ON p.stock_id = rs.id
        JOIN reports r ON rs.report_id = r.id
        WHERE p.track_date = ?
        ORDER BY p.return_pct DESC
    ''', (today,))
    
    rows = cursor.fetchall()
    
    # 按持有天数分组统计
    by_days = {}
    for row in rows:
        row = dict(row)
        days_held = row['days_held']
        
        if days_held not in by_days:
            by_days[days_held] = {
                "stocks": [],
                "total_return": 0,
                "win_count": 0,
            }
        
        by_days[days_held]["stocks"].append(row)
        by_days[days_held]["total_return"] += row['return_pct']
        if row['return_pct'] > 0:
            by_days[days_held]["win_count"] += 1
        
        report["details"].append(row)
    
    # 计算各时间段的平均收益和胜率
    for days_held, data in by_days.items():
        count = len(data["stocks"])
        report["summary"][f"T+{days_held}"] = {
            "count": count,
            "avg_return": round(data["total_return"] / count, 2) if count else 0,
            "win_rate": round(data["win_count"] / count * 100, 1) if count else 0,
        }
    
    conn.close()
    return report


def start_performance_scheduler():
    """
    启动收益更新定时任务
    每天15:30更新（收盘后）
    """
    try:
        import schedule
        import threading
        import time
        
        def run_scheduler():
            # 每天15:30更新收益
            schedule.every().day.at("15:30").do(update_all_performance)
            
            print("📅 收益跟踪定时任务已启动（每天15:30更新）")
            
            while True:
                schedule.run_pending()
                time.sleep(60)
        
        # 在后台线程运行
        thread = threading.Thread(target=run_scheduler, daemon=True)
        thread.start()
        
    except ImportError:
        print("⚠️ schedule模块未安装，定时任务无法启动")


if __name__ == "__main__":
    # 手动运行更新
    update_all_performance()
