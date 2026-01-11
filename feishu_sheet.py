# 飞书电子表格模块
# 用于将股票数据存储到飞书电子表格

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import time
import pandas as pd

# 保留最近几天的数据
KEEP_DAYS = 5

# ============ 配置区域 ============
# 请填入你的飞书应用信息
FEISHU_APP_ID = "cli_a9dcdb127938dcc0"  # 飞书应用 App ID
FEISHU_APP_SECRET = "Wk93xRR0CsfJozag4kb9FfvxEmrpzo8M"  # 飞书应用 App Secret
SPREADSHEET_TOKEN = "QTuNs07AwhNM2ytLrF4csBPXnLg"  # 电子表格 Token (从URL获取)
SHEET_ID = "5c033a"  # 工作表ID
WIKI_TOKEN = "QBo5wC0LliWwI8kOGG4cJ0ghnNf"  # 知识库文档 Token


class FeishuSheetClient:
    """飞书电子表格客户端"""
    
    BASE_URL = "https://open.feishu.cn/open-apis"
    
    def __init__(self, app_id: str = None, app_secret: str = None):
        self.app_id = app_id or FEISHU_APP_ID
        self.app_secret = app_secret or FEISHU_APP_SECRET
        self.tenant_token = None
        self.token_expire_time = 0
    
    def _get_tenant_token(self) -> str:
        """获取 tenant_access_token"""
        # 检查token是否过期
        if self.tenant_token and time.time() < self.token_expire_time - 60:
            return self.tenant_token
        
        url = f"{self.BASE_URL}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            resp = requests.post(url, json=payload, timeout=10)
            result = resp.json()
            
            if result.get("code") == 0:
                self.tenant_token = result.get("tenant_access_token")
                self.token_expire_time = time.time() + result.get("expire", 7200)
                print(f"✅ 获取飞书Token成功")
                return self.tenant_token
            else:
                print(f"❌ 获取Token失败: {result}")
                return None
        except Exception as e:
            print(f"❌ 获取Token异常: {e}")
            return None
    
    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """发送API请求"""
        token = self._get_tenant_token()
        if not token:
            return {"code": -1, "msg": "获取Token失败"}
        
        url = f"{self.BASE_URL}{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        try:
            if method.upper() == "GET":
                resp = requests.get(url, headers=headers, params=data, timeout=15)
            elif method.upper() == "POST":
                resp = requests.post(url, headers=headers, json=data, timeout=15)
            elif method.upper() == "PUT":
                resp = requests.put(url, headers=headers, json=data, timeout=15)
            else:
                return {"code": -1, "msg": f"不支持的方法: {method}"}
            
            return resp.json()
        except Exception as e:
            return {"code": -1, "msg": str(e)}

    
    def get_wiki_node_info(self, wiki_token: str) -> dict:
        """获取知识库节点信息，返回实际的文档类型和Token"""
        endpoint = f"/wiki/v2/spaces/get_node"
        result = self._request("GET", endpoint, {"token": wiki_token})
        
        if result.get("code") == 0:
            node = result.get("data", {}).get("node", {})
            obj_type = node.get("obj_type")  # sheet, doc, docx 等
            obj_token = node.get("obj_token")  # 实际的文档Token
            print(f"📄 Wiki节点类型: {obj_type}, Token: {obj_token}")
            return {"type": obj_type, "token": obj_token}
        print(f"❌ 获取Wiki节点失败: {result}")
        return {}
    
    def get_spreadsheet_info(self, spreadsheet_token: str) -> dict:
        """获取电子表格元信息"""
        endpoint = f"/sheets/v3/spreadsheets/{spreadsheet_token}"
        return self._request("GET", endpoint)
    
    def get_sheets(self, spreadsheet_token: str) -> List[dict]:
        """获取所有工作表信息"""
        endpoint = f"/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
        result = self._request("GET", endpoint)
        
        if result.get("code") == 0:
            return result.get("data", {}).get("sheets", [])
        return []
    
    def read_range(self, spreadsheet_token: str, range_str: str) -> List[List]:
        """
        读取指定范围的数据
        range_str 格式: "sheetId!A1:D10" 或 "Sheet1!A:D"
        """
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_str}"
        result = self._request("GET", endpoint)
        
        if result.get("code") == 0:
            return result.get("data", {}).get("valueRange", {}).get("values", [])
        print(f"❌ 读取数据失败: {result}")
        return []
    
    def write_range(self, spreadsheet_token: str, range_str: str, values: List[List]) -> bool:
        """
        写入数据到指定范围
        range_str 格式: "sheetId!A1:D10"
        values: 二维数组 [[row1_col1, row1_col2], [row2_col1, row2_col2]]
        """
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/values"
        payload = {
            "valueRange": {
                "range": range_str,
                "values": values
            }
        }
        
        result = self._request("PUT", endpoint, payload)
        
        if result.get("code") == 0:
            print(f"✅ 写入数据成功: {range_str}")
            return True
        print(f"❌ 写入数据失败: {result}")
        return False
    
    def append_rows(self, spreadsheet_token: str, sheet_id: str, values: List[List]) -> bool:
        """
        在表格末尾追加数据行
        """
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/values_append"
        payload = {
            "valueRange": {
                "range": f"{sheet_id}",
                "values": values
            }
        }
        
        result = self._request("POST", endpoint, payload)
        
        if result.get("code") == 0:
            print(f"✅ 追加数据成功: {len(values)} 行")
            return True
        print(f"❌ 追加数据失败: {result}")
        return False
    
    def batch_update(self, spreadsheet_token: str, value_ranges: List[dict]) -> bool:
        """
        批量更新多个范围
        value_ranges: [{"range": "sheet!A1:B2", "values": [[...], [...]]}, ...]
        """
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_update"
        payload = {
            "valueRanges": value_ranges
        }
        
        result = self._request("POST", endpoint, payload)
        
        if result.get("code") == 0:
            print(f"✅ 批量更新成功")
            return True
        print(f"❌ 批量更新失败: {result}")
        return False
    
    def delete_rows(self, spreadsheet_token: str, sheet_id: str, start_row: int, end_row: int) -> bool:
        """
        删除指定行
        start_row, end_row: 行号（从1开始）
        """
        # 先获取表格当前行数
        sheets = self.get_sheets(spreadsheet_token)
        max_rows = 200
        for s in sheets:
            if s.get("sheet_id") == sheet_id:
                max_rows = s.get("grid_properties", {}).get("row_count", 200)
                break
        
        # 确保不超出范围
        if end_row > max_rows:
            end_row = max_rows
        if start_row > end_row:
            return True
        
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range"
        payload = {
            "dimension": {
                "sheetId": sheet_id,
                "majorDimension": "ROWS",
                "startIndex": start_row,
                "endIndex": end_row + 1
            }
        }
        
        try:
            token = self._get_tenant_token()
            url = f"{self.BASE_URL}{endpoint}"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            resp = requests.delete(url, headers=headers, json=payload, timeout=15)
            result = resp.json()
            
            if result.get("code") == 0:
                print(f"✅ 删除行成功: {start_row}-{end_row}")
                return True
            # 静默处理范围错误
            if result.get("code") == 90202:
                return False
            print(f"❌ 删除行失败: {result}")
            return False
        except Exception as e:
            print(f"❌ 删除行异常: {e}")
            return False
    
    def set_style(self, spreadsheet_token: str, ranges: List[str], style: dict) -> bool:
        """
        设置单元格样式
        ranges: ["sheetId!A1:K1", ...] 或 ["sheetId!A1", ...]
        """
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/styles_batch_update"
        
        # 将多个range合并成一个请求
        data = []
        for range_str in ranges:
            data.append({
                "ranges": range_str,
                "style": style
            })
        
        payload = {"data": data}
        result = self._request("PUT", endpoint, payload)
        
        if result.get("code") == 0:
            return True
        # 静默处理范围错误（超出表格范围）
        if result.get("code") == 90202:
            return False
        print(f"❌ 设置样式失败: {result}")
        return False
    
    def set_column_width(self, spreadsheet_token: str, sheet_id: str, col_index: int, width: int) -> bool:
        """设置列宽"""
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range"
        payload = {
            "dimension": {
                "sheetId": sheet_id,
                "majorDimension": "COLUMNS",
                "startIndex": col_index,
                "endIndex": col_index + 1
            },
            "dimensionProperties": {
                "pixelSize": width
            }
        }
        result = self._request("PUT", endpoint, payload)
        return result.get("code") == 0
    
    def merge_cells(self, spreadsheet_token: str, range_str: str) -> bool:
        """合并单元格"""
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/merge_cells"
        payload = {
            "range": range_str,
            "mergeType": "MERGE_ALL"
        }
        result = self._request("POST", endpoint, payload)
        if result.get("code") == 0:
            return True
        # 静默处理错误
        return False
    
    def add_rows(self, spreadsheet_token: str, sheet_id: str, count: int) -> bool:
        """在表格末尾添加行"""
        endpoint = f"/sheets/v2/spreadsheets/{spreadsheet_token}/insert_dimension_range"
        payload = {
            "dimension": {
                "sheetId": sheet_id,
                "majorDimension": "ROWS",
                "startIndex": 200,  # 从第200行后添加
                "endIndex": 200 + count
            },
            "inheritStyle": "AFTER"
        }
        result = self._request("POST", endpoint, payload)
        if result.get("code") == 0:
            print(f"✅ 添加 {count} 行成功")
            return True
        return False



class StockDataSheet:
    """股票数据表格管理器"""
    
    def __init__(self, spreadsheet_token: str = None, sheet_id: str = None):
        self.client = FeishuSheetClient()
        self.spreadsheet_token = spreadsheet_token or SPREADSHEET_TOKEN
        self.sheet_id = sheet_id or SHEET_ID
        
        # 表头定义
        self.headers = [
            "日期", "时间", "题材名称", "题材涨幅%", "情绪阶段",
            "股票代码", "股票名称", "现价", "涨幅%", "角色", "信号"
        ]
    
    def init_sheet(self) -> bool:
        """初始化表格（写入表头并设置样式）"""
        if not self.sheet_id:
            sheets = self.client.get_sheets(self.spreadsheet_token)
            if sheets:
                self.sheet_id = sheets[0].get("sheet_id")
                print(f"📋 使用工作表: {sheets[0].get('title')} ({self.sheet_id})")
            else:
                print("❌ 未找到工作表")
                return False
        
        # 写入表头
        range_str = f"{self.sheet_id}!A1:K1"
        success = self.client.write_range(self.spreadsheet_token, range_str, [self.headers])
        
        if success:
            # 设置表头样式：蓝色背景、白色加粗字体、居中
            self.client.set_style(self.spreadsheet_token, [range_str], {
                "font": {"bold": True, "foreColor": "#FFFFFF"},
                "backColor": "#245BDB",
                "hAlign": 1,
                "vAlign": 1
            })
            
            # 设置列宽
            col_widths = [100, 80, 120, 80, 80, 90, 100, 70, 70, 60, 80]
            for i, width in enumerate(col_widths):
                self.client.set_column_width(self.spreadsheet_token, self.sheet_id, i, width)
            
            print("✅ 表头样式设置完成")
        
        return success
    
    def save_theme_data(self, theme_data: Dict) -> bool:
        """
        保存题材数据到表格
        theme_data: 从 theme_fetcher 获取的数据
        """
        if not self.sheet_id:
            sheets = self.client.get_sheets(self.spreadsheet_token)
            if sheets:
                self.sheet_id = sheets[0].get("sheet_id")
        
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M:%S")
        
        rows = []
        for theme_name, data in theme_data.items():
            theme_info = data.get("info", {})
            emotion = data.get("emotion", {})
            stocks = data.get("stocks", [])
            
            theme_change = theme_info.get("change_pct", 0) or 0
            stage = emotion.get("stage", "")
            
            for stock in stocks:
                row = [
                    date_str,
                    time_str,
                    theme_name,
                    theme_change,
                    stage,
                    stock.get("code", ""),
                    stock.get("name", ""),
                    stock.get("price", ""),
                    stock.get("change_pct", ""),
                    stock.get("role", ""),
                    stock.get("signal", "")
                ]
                rows.append(row)
        
        if rows:
            return self.client.append_rows(self.spreadsheet_token, self.sheet_id, rows)
        return True
    
    def save_daily_summary(self, theme_data: Dict, market_change: float = 0) -> bool:
        """保存每日汇总数据（使用pandas处理）"""
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M:%S")
        
        # 构建新数据
        new_rows = []
        for theme_name, data in theme_data.items():
            theme_info = data.get("info", {})
            emotion = data.get("emotion", {})
            stocks = data.get("stocks", [])
            
            theme_change = theme_info.get("change_pct", 0) or 0
            stage = emotion.get("stage", "")
            
            for stock in stocks[:5]:
                new_rows.append({
                    "日期": date_str,
                    "时间": time_str,
                    "题材名称": theme_name,
                    "题材涨幅%": theme_change,
                    "情绪阶段": stage,
                    "股票代码": stock.get("code", ""),
                    "股票名称": stock.get("name", ""),
                    "现价": stock.get("price", ""),
                    "涨幅%": stock.get("change_pct", ""),
                    "角色": stock.get("role", ""),
                    "信号": stock.get("signal", "")
                })
        
        if not new_rows:
            return True
        
        new_df = pd.DataFrame(new_rows)
        print(f"📊 新数据: {len(new_df)} 条")
        
        # 读取现有数据
        existing_data = self.client.read_range(self.spreadsheet_token, f"{self.sheet_id}!A:K")
        
        if existing_data and len(existing_data) > 1:
            # 转为DataFrame（跳过表头）
            old_df = pd.DataFrame(existing_data[1:], columns=self.headers)
            print(f"📋 现有数据: {len(old_df)} 条")
            
            # 确保日期列为字符串类型，处理空值
            old_df["日期"] = old_df["日期"].fillna("").astype(str)
            
            # 过滤掉无效的日期行（空值或非日期格式）
            old_df = old_df[old_df["日期"].str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)]
            
            # 过滤：删除当天数据 + 删除超过KEEP_DAYS的数据
            cutoff_date = (now - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
            old_df = old_df[old_df["日期"] != date_str]  # 删除当天
            old_df = old_df[old_df["日期"] >= cutoff_date]  # 删除过期
            print(f"📋 过滤后保留: {len(old_df)} 条历史数据")
            
            # 合并数据
            final_df = pd.concat([old_df, new_df], ignore_index=True)
        else:
            final_df = new_df
        
        print(f"📊 最终数据: {len(final_df)} 条")
        
        # 清空表格并重新写入
        return self._rewrite_sheet(final_df)
    
    def _rewrite_sheet(self, df: pd.DataFrame) -> bool:
        """清空表格并重新写入数据"""
        # 先读取当前行数
        existing = self.client.read_range(self.spreadsheet_token, f"{self.sheet_id}!A:A")
        if len(existing) > 1:
            # 清空数据区域（保留表头）
            # 用空值覆盖
            empty_rows = [[""] * 11 for _ in range(len(existing) - 1)]
            self.client.write_range(
                self.spreadsheet_token, 
                f"{self.sheet_id}!A2:K{len(existing)}", 
                empty_rows
            )
        
        # 写入表头
        self.client.write_range(
            self.spreadsheet_token,
            f"{self.sheet_id}!A1:K1",
            [self.headers]
        )
        
        if len(df) == 0:
            print("✅ 表格已清空")
            return True
        
        # 写入数据
        values = df.values.tolist()
        end_row = len(values) + 1
        range_str = f"{self.sheet_id}!A2:K{end_row}"
        
        success = self.client.write_range(self.spreadsheet_token, range_str, values)
        
        if success:
            # 应用样式
            self._apply_data_styles(2, end_row, values)
            # 合并同一题材的单元格
            self._merge_theme_cells(2, df)
        
        return success
    
    def _merge_theme_cells(self, start_row: int, df: pd.DataFrame):
        """合并同一题材的单元格（题材名称C列、题材涨幅D列、情绪阶段E列）"""
        print("🔗 合并题材单元格...")
        
        if len(df) == 0:
            return
        
        # 按题材分组，找出每个题材的起始行和结束行
        current_theme = None
        theme_start = start_row
        
        for i, row in df.iterrows():
            theme_name = row.get("题材名称", "")
            row_num = start_row + i
            
            if theme_name != current_theme:
                # 合并上一个题材的单元格
                if current_theme is not None and row_num - theme_start > 1:
                    # 合并C列（题材名称）
                    self.client.merge_cells(
                        self.spreadsheet_token, 
                        f"{self.sheet_id}!C{theme_start}:C{row_num - 1}"
                    )
                    # 合并D列（题材涨幅）
                    self.client.merge_cells(
                        self.spreadsheet_token, 
                        f"{self.sheet_id}!D{theme_start}:D{row_num - 1}"
                    )
                    # 合并E列（情绪阶段）
                    self.client.merge_cells(
                        self.spreadsheet_token, 
                        f"{self.sheet_id}!E{theme_start}:E{row_num - 1}"
                    )
                
                current_theme = theme_name
                theme_start = row_num
        
        # 处理最后一个题材
        if current_theme is not None:
            last_row = start_row + len(df) - 1
            if last_row - theme_start >= 1:
                self.client.merge_cells(
                    self.spreadsheet_token, 
                    f"{self.sheet_id}!C{theme_start}:C{last_row}"
                )
                self.client.merge_cells(
                    self.spreadsheet_token, 
                    f"{self.sheet_id}!D{theme_start}:D{last_row}"
                )
                self.client.merge_cells(
                    self.spreadsheet_token, 
                    f"{self.sheet_id}!E{theme_start}:E{last_row}"
                )
        
        print("✅ 单元格合并完成")
    
    def _apply_data_styles(self, start_row: int, end_row: int, rows: List[List]):
        """应用数据区域样式（渐变色效果）- 批量优化版"""
        print("🎨 应用表格样式...")
        
        try:
            # 设置整体数据区域样式：居中
            data_range = f"{self.sheet_id}!A{start_row}:K{end_row}"
            self.client.set_style(self.spreadsheet_token, [data_range], {
                "hAlign": 1,
                "vAlign": 1
            })
            
            # 收集所有样式设置，按颜色分组批量处理
            style_groups = {}  # {(backColor, foreColor, bold): [ranges]}
            
            for i, row in enumerate(rows):
                row_num = start_row + i
                
                # 题材涨幅 - 红色字体 (D列)
                theme_change = row[3] if len(row) > 3 else 0
                if isinstance(theme_change, (int, float)) and theme_change != 0:
                    text_color = "#D9534F" if theme_change > 0 else "#5CB85C"
                    key = ("none", text_color, True)
                    if key not in style_groups:
                        style_groups[key] = []
                    style_groups[key].append(f"{self.sheet_id}!D{row_num}:D{row_num}")
                
                # 股票涨幅 - 红色字体 (I列)
                stock_change = row[8] if len(row) > 8 else "0%"
                try:
                    change_val = float(str(stock_change).replace('%', '').replace('+', ''))
                    if change_val != 0:
                        text_color = "#D9534F" if change_val > 0 else "#5CB85C"
                        key = ("none", text_color, True)
                        if key not in style_groups:
                            style_groups[key] = []
                        style_groups[key].append(f"{self.sheet_id}!I{row_num}:I{row_num}")
                except:
                    pass
                
                # 角色列样式 (J列)
                role = row[9] if len(row) > 9 else ""
                if role == "龙头":
                    key = ("#FF6B6B", "#FFFFFF", True)
                    if key not in style_groups:
                        style_groups[key] = []
                    style_groups[key].append(f"{self.sheet_id}!J{row_num}:J{row_num}")
                elif role == "中军":
                    key = ("#FFB347", "#000000", True)
                    if key not in style_groups:
                        style_groups[key] = []
                    style_groups[key].append(f"{self.sheet_id}!J{row_num}:J{row_num}")
                elif role == "跟风":
                    key = ("#87CEEB", "#000000", False)
                    if key not in style_groups:
                        style_groups[key] = []
                    style_groups[key].append(f"{self.sheet_id}!J{row_num}:J{row_num}")
                
                # 信号列样式 (K列)
                signal = row[10] if len(row) > 10 else ""
                if "买" in signal or signal == "关注":
                    key = ("#4CAF50", "#FFFFFF", True)
                    if key not in style_groups:
                        style_groups[key] = []
                    style_groups[key].append(f"{self.sheet_id}!K{row_num}:K{row_num}")
                elif "卖" in signal or "减" in signal:
                    key = ("#F44336", "#FFFFFF", True)
                    if key not in style_groups:
                        style_groups[key] = []
                    style_groups[key].append(f"{self.sheet_id}!K{row_num}:K{row_num}")
                
                # 情绪阶段列样式 (E列)
                stage = row[4] if len(row) > 4 else ""
                stage_styles = {
                    "高潮": ("#FFCDD2", "#000000", True),  # 淡红色
                    "发酵": ("#BBDEFB", "#000000", True),  # 淡蓝色
                    "启动期": ("#C8E6C9", "#000000", True),  # 淡绿色
                    "主升期": ("#A5D6A7", "#000000", True),
                    "高潮期": ("#FFCDD2", "#000000", True),  # 淡红色
                    "分歧期": ("#FFF9C4", "#000000", True),  # 淡黄色
                    "退潮期": ("#FFCCBC", "#000000", True),  # 淡橙色
                }
                if stage in stage_styles:
                    key = stage_styles[stage]
                    if key not in style_groups:
                        style_groups[key] = []
                    style_groups[key].append(f"{self.sheet_id}!E{row_num}:E{row_num}")
            
            # 批量应用样式
            print(f"📝 共 {len(style_groups)} 组样式需要设置...")
            for (bg_color, text_color, bold), ranges in style_groups.items():
                if ranges:
                    if bg_color == "none":
                        # 只设置字体颜色，不设置背景
                        style = {"font": {"foreColor": text_color, "bold": bold}}
                    else:
                        style = {
                            "backColor": bg_color,
                            "font": {"foreColor": text_color, "bold": bold}
                        }
                    self.client.set_style(self.spreadsheet_token, ranges, style)
            
            print("✅ 样式应用完成")
        except Exception as e:
            print(f"⚠️ 样式设置异常: {e}")
    
    def _get_gradient_color(self, value: float) -> str:
        """根据涨跌幅返回渐变颜色"""
        # 涨：红色系 (浅红 -> 深红)
        # 跌：绿色系 (浅绿 -> 深绿)
        
        if value >= 10:
            return "#B71C1C"  # 深红 (涨停)
        elif value >= 7:
            return "#C62828"
        elif value >= 5:
            return "#E53935"
        elif value >= 3:
            return "#EF5350"
        elif value >= 1:
            return "#FFCDD2"  # 浅红
        elif value > 0:
            return "#FFEBEE"  # 很浅红
        elif value == 0:
            return "#FFFFFF"  # 白色
        elif value > -1:
            return "#E8F5E9"  # 很浅绿
        elif value > -3:
            return "#C8E6C9"  # 浅绿
        elif value > -5:
            return "#66BB6A"
        elif value > -7:
            return "#43A047"
        elif value > -10:
            return "#2E7D32"
        else:
            return "#1B5E20"  # 深绿 (跌停)
    
    def cleanup_old_data(self, keep_days: int = None) -> bool:
        """
        清理旧数据，只保留最近N天的数据
        """
        keep_days = keep_days or KEEP_DAYS
        cutoff_date = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        
        print(f"🧹 清理 {cutoff_date} 之前的数据...")
        
        # 读取所有数据
        range_str = f"{self.sheet_id}!A:A"
        all_data = self.client.read_range(self.spreadsheet_token, range_str)
        
        if not all_data:
            print("📋 表格为空，无需清理")
            return True
        
        # 找出需要删除的行（从后往前删，避免索引变化）
        rows_to_delete = []
        for i, row in enumerate(all_data):
            if i == 0:  # 跳过表头
                continue
            if row and len(row) > 0:
                date_val = str(row[0])
                # 检查是否是日期格式且早于截止日期
                if date_val < cutoff_date and len(date_val) == 10:
                    rows_to_delete.append(i + 1)  # 行号从1开始
        
        if not rows_to_delete:
            print(f"✅ 没有需要清理的旧数据")
            return True
        
        print(f"🗑️ 发现 {len(rows_to_delete)} 行旧数据需要清理")
        
        # 批量删除（从后往前删）
        rows_to_delete.sort(reverse=True)
        
        # 合并连续的行进行批量删除
        i = 0
        while i < len(rows_to_delete):
            end_row = rows_to_delete[i]
            start_row = end_row
            
            # 找连续的行
            while i + 1 < len(rows_to_delete) and rows_to_delete[i + 1] == start_row - 1:
                i += 1
                start_row = rows_to_delete[i]
            
            self.client.delete_rows(self.spreadsheet_token, self.sheet_id, start_row, end_row)
            i += 1
        
        print(f"✅ 旧数据清理完成")
        return True
    
    def cleanup_today_data(self) -> bool:
        """
        清理当天所有已有数据，为写入最新数据做准备
        """
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"🧹 清理今日({today})所有旧数据...")
        
        # 读取日期列
        range_str = f"{self.sheet_id}!A:A"
        all_data = self.client.read_range(self.spreadsheet_token, range_str)
        
        if not all_data or len(all_data) <= 1:
            print(f"✅ 今日暂无数据")
            return True
        
        # 找出今天的所有数据行
        rows_to_delete = []
        for i, row in enumerate(all_data):
            if i == 0:  # 跳过表头
                continue
            if row and len(row) > 0:
                date_val = str(row[0])
                if date_val == today:
                    rows_to_delete.append(i + 1)
        
        if not rows_to_delete:
            print(f"✅ 今日暂无数据")
            return True
        
        print(f"🗑️ 删除今日 {len(rows_to_delete)} 行旧数据，将写入最新数据")
        
        # 从后往前删除
        rows_to_delete.sort(reverse=True)
        
        # 批量删除连续行
        i = 0
        while i < len(rows_to_delete):
            end_row = rows_to_delete[i]
            start_row = end_row
            
            while i + 1 < len(rows_to_delete) and rows_to_delete[i + 1] == start_row - 1:
                i += 1
                start_row = rows_to_delete[i]
            
            self.client.delete_rows(self.spreadsheet_token, self.sheet_id, start_row, end_row)
            i += 1
        
        print(f"✅ 今日旧数据已清理")
        return True



def test_connection():
    """测试飞书连接"""
    print("🧪 测试飞书电子表格连接...")
    
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        print("❌ 请先配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
        return False
    
    client = FeishuSheetClient()
    token = client._get_tenant_token()
    
    if token:
        print(f"✅ Token获取成功!")
        
        # 如果配置了Wiki Token，先获取实际的表格Token
        if WIKI_TOKEN:
            print(f"📖 检测到Wiki配置，正在获取实际文档信息...")
            wiki_info = client.get_wiki_node_info(WIKI_TOKEN)
            if wiki_info:
                obj_type = wiki_info.get("type")
                obj_token = wiki_info.get("token")
                
                if obj_type == "sheet":
                    print(f"✅ 这是一个电子表格! Token: {obj_token}")
                    print(f"💡 请将 SPREADSHEET_TOKEN 设置为: {obj_token}")
                    
                    # 尝试获取表格信息
                    sheets = client.get_sheets(obj_token)
                    if sheets:
                        print(f"📋 工作表列表:")
                        for sheet in sheets:
                            print(f"   - {sheet.get('title')} (ID: {sheet.get('sheet_id')})")
                    return True
                else:
                    print(f"⚠️ 这不是电子表格，而是: {obj_type}")
                    print(f"💡 请在知识库中创建一个电子表格类型的页面")
            return False
        
        if SPREADSHEET_TOKEN:
            info = client.get_spreadsheet_info(SPREADSHEET_TOKEN)
            if info.get("code") == 0:
                title = info.get("data", {}).get("spreadsheet", {}).get("title", "")
                print(f"✅ 电子表格连接成功: {title}")
                
                sheets = client.get_sheets(SPREADSHEET_TOKEN)
                print(f"📋 工作表列表:")
                for sheet in sheets:
                    print(f"   - {sheet.get('title')} (ID: {sheet.get('sheet_id')})")
                return True
            else:
                print(f"❌ 无法访问电子表格: {info}")
        else:
            print("⚠️ 未配置 SPREADSHEET_TOKEN 或 WIKI_TOKEN")
        return True
    
    return False


def test_write():
    """测试写入数据"""
    print("🧪 测试写入数据...")
    
    sheet = StockDataSheet()
    
    # 初始化表头
    print("📝 写入表头...")
    sheet.init_sheet()
    
    # 写入测试数据
    test_data = {
        "测试题材": {
            "info": {"change_pct": 2.5},
            "emotion": {"stage": "启动期"},
            "stocks": [
                {"code": "000001", "name": "测试股票1", "price": 10.5, "change_pct": "3.2%", "role": "龙头", "signal": "关注"},
                {"code": "000002", "name": "测试股票2", "price": 8.3, "change_pct": "1.5%", "role": "中军", "signal": "观望"},
            ]
        }
    }
    
    success = sheet.save_daily_summary(test_data)
    
    if success:
        print("✅ 测试写入成功!")
    else:
        print("❌ 测试写入失败!")
    
    return success


def save_stock_data_to_sheet(cleanup: bool = True):
    """
    获取当前股票数据并保存到飞书表格
    使用pandas处理数据，自动去重和清理过期数据
    """
    print(f"\n{'='*60}")
    print(f"📊 开始保存股票数据到飞书表格...")
    print(f"{'='*60}")
    
    try:
        from theme_fetcher import fetch_all_themes_with_stocks
        from analyzer import analyze_and_format_stocks
        from emotion_cycle import calculate_theme_emotion
        from routes import get_market_index_change
        
        # 获取数据
        market_change = get_market_index_change()
        theme_data = fetch_all_themes_with_stocks(theme_limit=8)
        
        # 处理数据
        result = {}
        for theme_name, data in theme_data.items():
            stocks = data.get("stocks", [])
            theme_info = data.get("info", {})
            
            theme_change = theme_info.get("change_pct", 0) or 0
            emotion = calculate_theme_emotion(theme_info, stocks)
            formatted_stocks = analyze_and_format_stocks(stocks, market_change, theme_change)
            
            result[theme_name] = {
                "info": {"change_pct": theme_change},
                "emotion": {"stage": emotion["stage"]},
                "stocks": formatted_stocks
            }
        
        # 保存到表格（pandas会自动处理去重和过期数据）
        sheet = StockDataSheet()
        success = sheet.save_daily_summary(result, market_change)
        
        if success:
            print(f"✅ 股票数据已保存到飞书表格!")
        else:
            print(f"❌ 保存失败!")
        
        return success
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ 保存异常: {e}")
        return False


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "test":
            test_connection()
        elif cmd == "write":
            test_write()
        elif cmd == "save":
            save_stock_data_to_sheet()
        elif cmd == "cleanup":
            sheet = StockDataSheet()
            sheet.cleanup_old_data()
        else:
            print(f"未知命令: {cmd}")
    else:
        print("飞书电子表格模块")
        print("=" * 40)
        print("用法:")
        print("  python feishu_sheet.py test    - 测试连接")
        print("  python feishu_sheet.py write   - 测试写入")
        print("  python feishu_sheet.py save    - 保存当前股票数据")
        print("  python feishu_sheet.py cleanup - 清理旧数据")
        print()
        print(f"当前配置: 保留最近 {KEEP_DAYS} 天的数据")
