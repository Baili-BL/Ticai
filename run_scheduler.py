# 独立运行的定时推送脚本
# 用法: python run_scheduler.py
# 或者用 nohup: nohup python run_scheduler.py > scheduler.log 2>&1 &

from feishu_pusher import push_daily_stock_report, send_feishu_text
import schedule
import time
from datetime import datetime

def main():
    print(f"🚀 定时推送服务启动")
    print(f"📅 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 启动时发送通知，确认服务正常
    send_feishu_text(f"✅ 股票日报推送服务已启动\n⏰ 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n📅 推送时间: 每天 11:00、20:00")
    
    # 每天11:00推送（午盘）
    schedule.every().day.at("11:00").do(push_daily_stock_report)
    # 每天20:00推送（收盘总结）
    schedule.every().day.at("20:00").do(push_daily_stock_report)
    
    print(f"📅 已设置每天 11:00、20:00 推送")
    print(f"⏳ 等待执行中...")
    
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
