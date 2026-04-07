#!/usr/bin/env python3
import asyncio
import json
import os
import sys
import requests
from datetime import datetime
from playwright.async_api import async_playwright

NOTION_URL = "https://road-halibut-51b.notion.site/332e12d2634480f6b247fccac41119fa?v=332e12d2634480749135000cb753c8c8"
WECOM_WEBHOOK = os.environ.get("WECOM_WEBHOOK")
STATE_FILE = "state.json"


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def send_wecom(content):
    if not WECOM_WEBHOOK:
        print("警告：WECOM_WEBHOOK 未设置")
        return
    data = {"msgtype": "text", "text": {"content": content}}
    try:
        resp = requests.post(WECOM_WEBHOOK, json=data, timeout=10)
        print(f"企业微信通知: {resp.json()}")
    except Exception as e:
        print(f"企业微信通知发送失败: {e}")


async def scrape_table():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()

        print("加载页面...")
        try:
            await page.goto(NOTION_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_selector(".notion-table-view", timeout=60000)
        except Exception as e:
            print(f"页面加载出错: {e}")
            await page.screenshot(path="debug.png")
            await browser.close()
            raise

        # 滚动到底部确保所有行都已加载
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(3)

        data = await page.evaluate("""() => {
            const headerCells = document.querySelectorAll('.notion-table-view-header-cell');
            const headers = Array.from(headerCells).map(h => h.textContent.trim());

            if (headers.length === 0) {
                return { error: 'headers_not_found' };
            }

            const colIdx = {};
            ['币种', '做单方向', '交易状态', '添加时间'].forEach(col => {
                colIdx[col] = headers.indexOf(col);
            });

            const allRows = document.querySelectorAll('.notion-table-view-row');
            const results = [];

            allRows.forEach(row => {
                const cells = row.querySelectorAll('.notion-table-view-cell');
                const cellArr = Array.from(cells).map(c => c.textContent.trim());

                const record = {};
                for (const [col, idx] of Object.entries(colIdx)) {
                    record[col] = (idx >= 0 && idx < cellArr.length) ? cellArr[idx] : '';
                }

                if (record['币种'] && record['添加时间']) {
                    results.push(record);
                }
            });

            return { headers, results };
        }""")

        await browser.close()

        if isinstance(data, dict) and "error" in data:
            raise Exception(f"页面解析失败: {data['error']}")

        print(f"找到列: {data.get('headers', [])}")
        return data.get("results", [])


def make_row_id(row):
    return f"{row.get('币种', '').strip()}_{row.get('添加时间', '').strip()}"


async def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] 开始检查...")

    try:
        current_rows = await scrape_table()
    except Exception as e:
        print(f"抓取失败: {e}")
        sys.exit(1)

    print(f"共抓取 {len(current_rows)} 条记录")

    prev_state = load_state()

    current_state = {}
    for row in current_rows:
        row_id = make_row_id(row)
        if row_id and row_id != "_":
            current_state[row_id] = row

    # 首次运行
    if not prev_state:
        save_state(current_state)
        send_wecom(
            f"✅ [Notion监控] 启动成功\n"
            f"已读取 {len(current_state)} 条交易记录\n"
            f"将每15分钟检查一次新增行和状态变更"
        )
        print("首次运行，初始状态已保存")
        return

    messages = []

    for row_id, row in current_state.items():
        if row_id not in prev_state:
            msg = (
                f"📈 [Notion监控] 新增交易记录\n"
                f"币种：{row.get('币种', '-')}  方向：{row.get('做单方向', '-')}\n"
                f"状态：{row.get('交易状态', '-')}\n"
                f"时间：{row.get('添加时间', '-')}"
            )
            messages.append(msg)
            print(f"新增: {row_id}")
        else:
            old_status = prev_state[row_id].get("交易状态", "")
            new_status = row.get("交易状态", "")
            if old_status and new_status and old_status != new_status:
                msg = (
                    f"🔄 [Notion监控] 交易状态变更\n"
                    f"币种：{row.get('币种', '-')}  方向：{row.get('做单方向', '-')}\n"
                    f"{old_status} → {new_status}\n"
                    f"时间：{row.get('添加时间', '-')}"
                )
                messages.append(msg)
                print(f"状态变更: {row_id}: {old_status} → {new_status}")

    if messages:
        for msg in messages:
            send_wecom(msg)
        print(f"发送了 {len(messages)} 条通知")
    else:
        print("无变化")

    save_state(current_state)
    print("完成")


if __name__ == "__main__":
    asyncio.run(main())
