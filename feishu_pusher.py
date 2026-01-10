# 飞书群机器人推送模块
# 每天20点推送股票数据到飞书群
import requests
import json
from datetime import datetime
from typing import Dict, List, Optional
import schedule
import time
import threading

# 飞书Webhook地址
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/4dbfb98d-927c-4937-b513-c82605b75c15"


def send_feishu_text(text: str) -> bool:
    """发送纯文本消息到飞书"""
    payload = {
        "msg_type": "text",
        "content": {
            "text": text
        }
    }
    return _send_to_feishu(payload)


def send_feishu_rich(title: str, content: List[List[dict]]) -> bool:
    """
    发送富文本消息到飞书
    content格式: [[{tag: "text", text: "xxx"}, {tag: "a", text: "链接", href: "url"}], [...]]
    每个内层数组代表一行
    """
    payload = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": content
                }
            }
        }
    }
    return _send_to_feishu(payload)


def _send_to_feishu(payload: dict) -> bool:
    """发送消息到飞书"""
    try:
        headers = {"Content-Type": "application/json; charset=utf-8"}
        response = requests.post(
            FEISHU_WEBHOOK_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=10
        )
        result = response.json()
        if result.get("code") == 0:
            print(f"✅ 飞书消息发送成功")
            return True
        else:
            print(f"❌ 飞书消息发送失败: {result}")
            return False
    except Exception as e:
        print(f"❌ 飞书消息发送异常: {e}")
        return False


def format_stock_message(theme_data: Dict) -> List[List[dict]]:
    """
    格式化股票数据为飞书富文本格式
    """
    content = []
    
    for theme_name, data in theme_data.items():
        theme_info = data.get("info", {})
        emotion = data.get("emotion", {})
        change_pct = theme_info.get("change_pct", 0)
        stage = emotion.get("stage", "")
        
        change_icon = "📈" if change_pct > 0 else "📉" if change_pct < 0 else "➖"
        
        content.append([
            {"tag": "text", "text": f"\n{'='*30}\n"},
        ])
        content.append([
            {"tag": "text", "text": f"🔥 {theme_name} "},
            {"tag": "text", "text": f"{change_icon} {change_pct:+.2f}% "},
            {"tag": "text", "text": f"| {stage}"},
        ])
        
        stocks = data.get("stocks", [])
        for i, stock in enumerate(stocks[:5], 1):
            name = stock.get("name", "")
            code = stock.get("code", "")
            price = stock.get("price", "-")
            stock_change = stock.get("change_pct", "0%")
            role = stock.get("role", "")
            signal = stock.get("signal", "")
            
            role_icon = {"龙头": "🐉", "中军": "⚔️", "低吸": "💰"}.get(role, "")
            
            content.append([
                {"tag": "text", "text": f"  {i}. {role_icon}{name}({code}) "},
                {"tag": "text", "text": f"{price}元 {stock_change}"},
            ])
            if signal and signal != "观望":
                content.append([
                    {"tag": "text", "text": f"     💡 {signal}"},
                ])
    
    return content


def build_daily_report(theme_data: Dict, market_change: float = 0) -> tuple:
    """
    构建每日股票报告
    返回: (标题, 内容)
    """
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    
    market_icon = "📈" if market_change > 0 else "📉" if market_change < 0 else "➖"
    title = f"📊 {date_str} 热门题材日报 {market_icon}大盘{market_change:+.2f}%"
    
    content = []
    
    theme_count = len(theme_data)
    content.append([
        {"tag": "text", "text": f"今日共监控 {theme_count} 个热门题材\n"},
    ])
    
    stock_content = format_stock_message(theme_data)
    content.extend(stock_content)
    
    content.append([
        {"tag": "text", "text": f"\n{'='*30}\n"},
    ])
    content.append([
        {"tag": "text", "text": "⚠️ 以上数据仅供参考，不构成投资建议"},
    ])
    
    return title, content


def push_daily_stock_report():
    """
    推送每日股票报告到飞书
    """
    print(f"\n{'='*60}")
    print(f"🚀 开始推送每日股票报告到飞书...")
    print(f"{'='*60}")
    
    try:
        # 获取股票数据
        from theme_fetcher import fetch_all_themes_with_stocks
        from analyzer import analyze_and_format_stocks
        from emotion_cycle import calculate_theme_emotion
        from routes import get_market_index_change
        
        # 获取大盘数据
        market_change = get_market_index_change()
        
        # 获取题材数据
        theme_data = fetch_all_themes_with_stocks(theme_limit=8)
        
        # 处理数据
        result = {}
        for theme_name, data in theme_data.items():
            stocks = data.get("stocks", [])
            theme_info = data.get("info", {})
            history = data.get("history", {})
            
            theme_change = theme_info.get("change_pct", 0) or 0
            emotion = calculate_theme_emotion(theme_info, stocks)
            formatted_stocks = analyze_and_format_stocks(stocks, market_change, theme_change)
            
            result[theme_name] = {
                "info": {
                    "change_pct": theme_change,
                },
                "emotion": {
                    "stage": emotion["stage"],
                },
                "stocks": formatted_stocks
            }
        
        # 构建报告
        title, content = build_daily_report(result, market_change)
        
        # 发送到飞书
        success = send_feishu_rich(title, content)
        
        if success:
            print(f"✅ 每日股票报告推送成功!")
        else:
            print(f"❌ 每日股票报告推送失败!")
            
        return success
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ 推送异常: {e}")
        # 发送错误通知
        send_feishu_text(f"⚠️ 股票日报推送失败: {str(e)}")
        return False


def start_scheduler():
    """
    启动定时任务调度器
    """
    # 清除旧任务
    schedule.clear()
    
    # 每天11:00推送（午盘）
    schedule.every().day.at("11:00").do(push_daily_stock_report)
    # 每天20:00推送（收盘总结）
    schedule.every().day.at("20:00").do(push_daily_stock_report)
    
    print(f"📅 定时任务已设置: 11:00、20:00 推送股票日报")
    print(f"📅 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(30)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    return scheduler_thread


def test_push():
    """测试推送功能"""
    print("🧪 测试飞书推送...")
    
    # 测试简单文本
    success = send_feishu_text("🧪 这是一条测试消息，股票日报推送功能已就绪！")
    
    if success:
        print("✅ 测试消息发送成功!")
    else:
        print("❌ 测试消息发送失败!")
    
    return success


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # 测试推送
            test_push()
        elif sys.argv[1] == "push":
            # 立即推送一次
            push_daily_stock_report()
        elif sys.argv[1] == "schedule":
            # 启动定时任务
            start_scheduler()
            print("定时任务运行中，按 Ctrl+C 退出...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n定时任务已停止")
    else:
        print("用法:")
        print("  python feishu_pusher.py test     - 测试推送")
        print("  python feishu_pusher.py push     - 立即推送一次")
        print("  python feishu_pusher.py schedule - 启动定时任务(每天20:00)")
