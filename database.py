# 数据库模块 - SQLite存储推荐报表和收益跟踪
import sqlite3
import os
from datetime import datetime, date
from typing import List, Dict, Optional
import json

# 数据库文件路径
DB_PATH = os.path.join(os.path.dirname(__file__), "ticai.db")


def get_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # 返回字典格式
    return conn


def migrate_database():
    """
    数据库迁移 - 为旧表添加新字段
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 检查 recommended_stocks 表是否有新字段
    cursor.execute("PRAGMA table_info(recommended_stocks)")
    columns = [col[1] for col in cursor.fetchall()]
    
    # 添加缺失的字段
    new_columns = [
        ("open_change", "REAL DEFAULT 0"),
        ("is_buyable", "INTEGER DEFAULT 1"),
        ("unbuyable_reason", "TEXT"),
    ]
    
    for col_name, col_type in new_columns:
        if col_name not in columns:
            try:
                cursor.execute(f"ALTER TABLE recommended_stocks ADD COLUMN {col_name} {col_type}")
                print(f"✅ 已添加字段: {col_name}")
            except Exception as e:
                print(f"添加字段 {col_name} 失败: {e}")
    
    conn.commit()
    conn.close()


def init_database():
    """
    初始化数据库表结构
    
    表结构说明：
    1. reports - 每日推荐报表
    2. recommended_stocks - 推荐的股票详情
    3. performance - 收益跟踪记录
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 创建报表表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date DATE NOT NULL UNIQUE,
            market_change REAL DEFAULT 0,
            themes_count INTEGER DEFAULT 0,
            stocks_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 创建推荐股票表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recommended_stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER NOT NULL,
            theme_name TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            recommend_price REAL,
            change_pct REAL,
            score INTEGER,
            role TEXT,
            role_reason TEXT,
            signal TEXT,
            volume_level TEXT,
            strength TEXT,
            is_weak_to_strong INTEGER DEFAULT 0,
            is_front_runner INTEGER DEFAULT 0,
            front_runner_tags TEXT,
            market_cap REAL,
            amount REAL,
            turnover_rate REAL,
            open_change REAL DEFAULT 0,
            is_buyable INTEGER DEFAULT 1,
            unbuyable_reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (report_id) REFERENCES reports(id)
        )
    ''')
    
    # 创建收益跟踪表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER NOT NULL,
            track_date DATE NOT NULL,
            days_held INTEGER NOT NULL,
            current_price REAL,
            return_pct REAL,
            is_trading_day INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (stock_id) REFERENCES recommended_stocks(id),
            UNIQUE(stock_id, track_date)
        )
    ''')
    
    # 创建索引提高查询效率
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_reports_date ON reports(report_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_report ON recommended_stocks(report_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stocks_code ON recommended_stocks(stock_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_performance_stock ON performance(stock_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_performance_date ON performance(track_date)')
    
    conn.commit()
    conn.close()
    
    # 执行数据库迁移（为旧表添加新字段）
    migrate_database()
    
    print("✅ 数据库初始化完成")


def save_report(report_date: date, market_change: float, themes_data: Dict) -> int:
    """
    保存每日推荐报表
    
    参数:
        report_date: 报表日期
        market_change: 大盘涨跌幅
        themes_data: 题材数据（从/api/all返回的数据）
    
    返回:
        report_id: 报表ID
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # 计算统计数据
        themes_count = len(themes_data)
        stocks_count = sum(len(t.get("stocks", [])) for t in themes_data.values())
        
        # 插入或更新报表记录
        cursor.execute('''
            INSERT OR REPLACE INTO reports (report_date, market_change, themes_count, stocks_count)
            VALUES (?, ?, ?, ?)
        ''', (report_date, market_change, themes_count, stocks_count))
        
        report_id = cursor.lastrowid
        
        # 如果是更新，先删除旧的股票记录
        cursor.execute('SELECT id FROM reports WHERE report_date = ?', (report_date,))
        row = cursor.fetchone()
        if row:
            report_id = row['id']
            cursor.execute('DELETE FROM recommended_stocks WHERE report_id = ?', (report_id,))
        
        # 保存推荐股票
        for theme_name, theme_data in themes_data.items():
            stocks = theme_data.get("stocks", [])
            for stock in stocks:
                # 提取价格数值
                price_str = stock.get("price", "0")
                try:
                    price = float(price_str) if price_str and price_str != "-" else 0
                except:
                    price = 0
                
                # 提取涨跌幅数值
                change_pct = stock.get("change_pct_num", 0) or 0
                
                # 提取换手率数值
                turnover_str = stock.get("turnover_rate", "0%")
                try:
                    turnover = float(turnover_str.replace("%", "")) if turnover_str else 0
                except:
                    turnover = 0
                
                # 提取开盘涨幅（用于判断是否可买入）
                open_change = stock.get("open_change", 0) or 0
                
                # 判断是否可买入（排除买不到的情况）
                is_buyable = 1
                unbuyable_reason = ""
                
                # 情况1：一字涨停（开盘涨幅>=9.5%，基本买不到）
                if open_change >= 9.5:
                    is_buyable = 0
                    unbuyable_reason = "一字涨停"
                # 情况2：竞价涨停（开盘涨幅>=7%且当前涨停，很难买到）
                elif open_change >= 7 and change_pct >= 9.9:
                    is_buyable = 0
                    unbuyable_reason = "竞价涨停"
                # 情况3：早盘秒板（开盘涨幅>=5%且很快涨停，难买）
                elif open_change >= 5 and change_pct >= 9.9:
                    is_buyable = 0
                    unbuyable_reason = "高开秒板"
                
                cursor.execute('''
                    INSERT INTO recommended_stocks (
                        report_id, theme_name, stock_code, stock_name,
                        recommend_price, change_pct, score, role, role_reason,
                        signal, volume_level, strength, is_weak_to_strong,
                        is_front_runner, front_runner_tags, market_cap, amount, turnover_rate,
                        open_change, is_buyable, unbuyable_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    report_id,
                    theme_name,
                    stock.get("code", ""),
                    stock.get("name", ""),
                    price,
                    change_pct,
                    stock.get("score", 0),
                    stock.get("role", ""),
                    stock.get("role_reason", ""),
                    stock.get("signal", ""),
                    stock.get("volume_level", ""),
                    stock.get("strength", ""),
                    1 if stock.get("is_weak_to_strong") else 0,
                    1 if stock.get("is_front_runner") else 0,
                    json.dumps(stock.get("front_runner_tags", []), ensure_ascii=False),
                    stock.get("market_cap", 0) or 0,
                    stock.get("amount", 0) or 0,
                    turnover,
                    open_change,
                    is_buyable,
                    unbuyable_reason
                ))
        
        conn.commit()
        print(f"✅ 报表保存成功: {report_date}, 共{themes_count}个题材, {stocks_count}只股票")
        return report_id
        
    except Exception as e:
        conn.rollback()
        print(f"❌ 保存报表失败: {e}")
        raise
    finally:
        conn.close()


def get_report_by_date(report_date: date) -> Optional[Dict]:
    """获取指定日期的报表"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM reports WHERE report_date = ?', (report_date,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        return None
    
    report = dict(row)
    
    # 获取推荐股票
    cursor.execute('''
        SELECT * FROM recommended_stocks WHERE report_id = ?
        ORDER BY theme_name, score DESC
    ''', (report['id'],))
    
    stocks = [dict(r) for r in cursor.fetchall()]
    report['stocks'] = stocks
    
    # 按题材分组
    themes = {}
    for stock in stocks:
        theme_name = stock['theme_name']
        if theme_name not in themes:
            themes[theme_name] = []
        themes[theme_name].append(stock)
    report['themes'] = themes
    
    conn.close()
    return report


def get_recent_reports(limit: int = 30) -> List[Dict]:
    """获取最近的报表列表"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM reports 
        ORDER BY report_date DESC 
        LIMIT ?
    ''', (limit,))
    
    reports = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return reports


def get_stocks_for_tracking(days_ago: int = 5, only_buyable: bool = True) -> List[Dict]:
    """
    获取需要跟踪收益的股票
    获取最近N天推荐的股票，用于更新收益
    
    参数:
        days_ago: 获取最近多少天的数据
        only_buyable: 是否只返回可买入的股票（排除一字板等买不到的）
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if only_buyable:
        cursor.execute('''
            SELECT rs.*, r.report_date
            FROM recommended_stocks rs
            JOIN reports r ON rs.report_id = r.id
            WHERE r.report_date >= date('now', ? || ' days')
              AND rs.is_buyable = 1
            ORDER BY r.report_date DESC
        ''', (-days_ago,))
    else:
        cursor.execute('''
            SELECT rs.*, r.report_date
            FROM recommended_stocks rs
            JOIN reports r ON rs.report_id = r.id
            WHERE r.report_date >= date('now', ? || ' days')
            ORDER BY r.report_date DESC
        ''', (-days_ago,))
    
    stocks = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return stocks


def save_performance(stock_id: int, track_date: date, days_held: int, 
                    current_price: float, return_pct: float, is_trading_day: bool = True):
    """保存收益跟踪记录"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO performance 
            (stock_id, track_date, days_held, current_price, return_pct, is_trading_day)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (stock_id, track_date, days_held, current_price, return_pct, 1 if is_trading_day else 0))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"❌ 保存收益记录失败: {e}")
    finally:
        conn.close()


def get_stock_performance(stock_id: int) -> List[Dict]:
    """获取单只股票的收益跟踪记录"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM performance 
        WHERE stock_id = ?
        ORDER BY track_date ASC
    ''', (stock_id,))
    
    records = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return records


def get_performance_summary(days: int = 30, only_buyable: bool = True) -> Dict:
    """
    获取收益统计摘要
    
    统计最近N天的推荐股票表现：
    - 总体胜率（盈利股票占比）
    - 平均收益率
    - T+1、T+3、T+5收益统计
    - 按角色（龙头/中军/低吸）分类统计
    
    参数:
        days: 统计最近多少天
        only_buyable: 是否只统计可买入的股票（排除一字板等买不到的）
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    summary = {
        "total_stocks": 0,
        "buyable_stocks": 0,
        "unbuyable_stocks": 0,
        "win_count": 0,
        "win_rate": 0,
        "avg_return": 0,
        "by_days": {},
        "by_role": {},           # 按角色：龙头/中军/低吸/跟风
        "by_score": {},          # 按评分：90+/80-89/70-79/<70
        "by_volume": {},         # 按量能：爆量/放量/中量/缩量/地量
        "by_strength": {},       # 按强度：涨停/强势/偏强/震荡/偏弱
        "by_weak_to_strong": {}, # 弱转强 vs 非弱转强
        "by_front_runner": {},   # 前排强度 vs 普通
        "best_stocks": [],
        "worst_stocks": [],
        "unbuyable_list": [],
    }
    
    # 获取有收益记录的股票（只统计可买入的）
    buyable_filter = "AND rs.is_buyable = 1" if only_buyable else ""
    cursor.execute(f'''
        SELECT rs.*, r.report_date,
               p.days_held, p.return_pct, p.current_price, p.track_date
        FROM recommended_stocks rs
        JOIN reports r ON rs.report_id = r.id
        LEFT JOIN performance p ON rs.id = p.stock_id
        WHERE r.report_date >= date('now', ? || ' days')
        {buyable_filter}
        ORDER BY rs.id, p.days_held
    ''', (-days,))
    
    rows = cursor.fetchall()
    
    # 统计数据
    stock_returns = {}  # {stock_id: {days_held: return_pct}}
    stock_info = {}  # {stock_id: stock_data}
    
    for row in rows:
        row = dict(row)
        stock_id = row['id']
        
        if stock_id not in stock_info:
            stock_info[stock_id] = row
            stock_returns[stock_id] = {}
        
        if row['days_held'] is not None:
            stock_returns[stock_id][row['days_held']] = row['return_pct']
    
    summary["total_stocks"] = len(stock_info)
    
    # 计算各持有天数的统计
    for days_held in [1, 2, 3, 5]:
        returns = [sr.get(days_held, 0) for sr in stock_returns.values() if days_held in sr]
        if returns:
            summary["by_days"][f"T+{days_held}"] = {
                "count": len(returns),
                "avg_return": round(sum(returns) / len(returns), 2),
                "win_rate": round(len([r for r in returns if r > 0]) / len(returns) * 100, 1),
                "max_return": round(max(returns), 2),
                "min_return": round(min(returns), 2),
            }
    
    # 按角色统计（使用T+1收益）
    role_returns = {}
    for stock_id, info in stock_info.items():
        role = info.get('role', '跟风') or '跟风'
        if role not in role_returns:
            role_returns[role] = []
        if 1 in stock_returns[stock_id]:
            role_returns[role].append(stock_returns[stock_id][1])
    
    for role, returns in role_returns.items():
        if returns:
            summary["by_role"][role] = {
                "count": len(returns),
                "avg_return": round(sum(returns) / len(returns), 2),
                "win_rate": round(len([r for r in returns if r > 0]) / len(returns) * 100, 1),
            }
    
    # 按评分区间统计
    score_returns = {"90+强推": [], "80-89可买": [], "70-79观察": [], "<70弱": []}
    for stock_id, info in stock_info.items():
        score = info.get('score', 0) or 0
        if score >= 90:
            key = "90+强推"
        elif score >= 80:
            key = "80-89可买"
        elif score >= 70:
            key = "70-79观察"
        else:
            key = "<70弱"
        if 1 in stock_returns[stock_id]:
            score_returns[key].append(stock_returns[stock_id][1])
    
    for label, returns in score_returns.items():
        if returns:
            summary["by_score"][label] = {
                "count": len(returns),
                "avg_return": round(sum(returns) / len(returns), 2),
                "win_rate": round(len([r for r in returns if r > 0]) / len(returns) * 100, 1),
            }
    
    # 按量能统计
    volume_returns = {}
    for stock_id, info in stock_info.items():
        volume = info.get('volume_level', '未知') or '未知'
        if volume not in volume_returns:
            volume_returns[volume] = []
        if 1 in stock_returns[stock_id]:
            volume_returns[volume].append(stock_returns[stock_id][1])
    
    for volume, returns in volume_returns.items():
        if returns:
            summary["by_volume"][volume] = {
                "count": len(returns),
                "avg_return": round(sum(returns) / len(returns), 2),
                "win_rate": round(len([r for r in returns if r > 0]) / len(returns) * 100, 1),
            }
    
    # 按强度统计
    strength_returns = {}
    for stock_id, info in stock_info.items():
        strength = info.get('strength', '未知') or '未知'
        if strength not in strength_returns:
            strength_returns[strength] = []
        if 1 in stock_returns[stock_id]:
            strength_returns[strength].append(stock_returns[stock_id][1])
    
    for strength, returns in strength_returns.items():
        if returns:
            summary["by_strength"][strength] = {
                "count": len(returns),
                "avg_return": round(sum(returns) / len(returns), 2),
                "win_rate": round(len([r for r in returns if r > 0]) / len(returns) * 100, 1),
            }
    
    # 弱转强 vs 非弱转强
    weak_to_strong_returns = {"弱转强": [], "非弱转强": []}
    for stock_id, info in stock_info.items():
        is_wts = info.get('is_weak_to_strong', 0)
        key = "弱转强" if is_wts else "非弱转强"
        if 1 in stock_returns[stock_id]:
            weak_to_strong_returns[key].append(stock_returns[stock_id][1])
    
    for label, returns in weak_to_strong_returns.items():
        if returns:
            summary["by_weak_to_strong"][label] = {
                "count": len(returns),
                "avg_return": round(sum(returns) / len(returns), 2),
                "win_rate": round(len([r for r in returns if r > 0]) / len(returns) * 100, 1),
            }
    
    # 前排强度 vs 普通
    front_runner_returns = {"前排强势": [], "普通": []}
    for stock_id, info in stock_info.items():
        is_fr = info.get('is_front_runner', 0)
        key = "前排强势" if is_fr else "普通"
        if 1 in stock_returns[stock_id]:
            front_runner_returns[key].append(stock_returns[stock_id][1])
    
    for label, returns in front_runner_returns.items():
        if returns:
            summary["by_front_runner"][label] = {
                "count": len(returns),
                "avg_return": round(sum(returns) / len(returns), 2),
                "win_rate": round(len([r for r in returns if r > 0]) / len(returns) * 100, 1),
            }
    
    # 总体胜率（使用T+1收益）
    t1_returns = [sr.get(1, 0) for sr in stock_returns.values() if 1 in sr]
    if t1_returns:
        summary["win_count"] = len([r for r in t1_returns if r > 0])
        summary["win_rate"] = round(summary["win_count"] / len(t1_returns) * 100, 1)
        summary["avg_return"] = round(sum(t1_returns) / len(t1_returns), 2)
    
    # 最佳和最差股票（使用T+1收益）
    stock_t1_list = []
    for stock_id, info in stock_info.items():
        if 1 in stock_returns[stock_id]:
            stock_t1_list.append({
                "code": info['stock_code'],
                "name": info['stock_name'],
                "theme": info['theme_name'],
                "role": info['role'],
                "recommend_price": info['recommend_price'],
                "return_pct": stock_returns[stock_id][1],
                "report_date": info['report_date'],
            })
    
    stock_t1_list.sort(key=lambda x: x['return_pct'], reverse=True)
    summary["best_stocks"] = stock_t1_list[:5]
    summary["worst_stocks"] = stock_t1_list[-5:][::-1] if len(stock_t1_list) >= 5 else stock_t1_list[::-1]
    
    # 统计买不到的股票
    cursor.execute('''
        SELECT rs.stock_code, rs.stock_name, rs.theme_name, rs.role,
               rs.open_change, rs.change_pct, rs.unbuyable_reason, r.report_date
        FROM recommended_stocks rs
        JOIN reports r ON rs.report_id = r.id
        WHERE r.report_date >= date('now', ? || ' days')
          AND rs.is_buyable = 0
        ORDER BY r.report_date DESC
    ''', (-days,))
    
    unbuyable_rows = cursor.fetchall()
    summary["unbuyable_stocks"] = len(unbuyable_rows)
    summary["buyable_stocks"] = summary["total_stocks"]
    summary["total_stocks"] = summary["total_stocks"] + summary["unbuyable_stocks"]
    
    # 买不到的股票列表（最多显示10条）
    summary["unbuyable_list"] = [
        {
            "code": row["stock_code"],
            "name": row["stock_name"],
            "theme": row["theme_name"],
            "role": row["role"],
            "open_change": row["open_change"],
            "change_pct": row["change_pct"],
            "reason": row["unbuyable_reason"],
            "report_date": row["report_date"],
        }
        for row in unbuyable_rows[:10]
    ]
    
    conn.close()
    return summary


def get_stock_history(stock_code: str, limit: int = 10) -> List[Dict]:
    """获取某只股票的历史推荐记录"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT rs.*, r.report_date
        FROM recommended_stocks rs
        JOIN reports r ON rs.report_id = r.id
        WHERE rs.stock_code = ?
        ORDER BY r.report_date DESC
        LIMIT ?
    ''', (stock_code, limit))
    
    records = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return records


# 数据库初始化（首次导入时执行）
if not os.path.exists(DB_PATH):
    init_database()
else:
    # 已有数据库，执行迁移检查
    migrate_database()
