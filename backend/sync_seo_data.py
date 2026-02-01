"""
SEO 数据同步脚本
从 Google Search Console API 拉取数据并保存到本地数据库（按日期）

运行方式：
python sync_seo_data.py
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def get_db_connection():
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ads_data.sqlite')
    return sqlite3.connect(db_path)

def fetch_meta(url):
    """爬取单个URL的Meta信息"""
    import requests
    from bs4 import BeautifulSoup
    
    meta_title = ""
    meta_description = ""
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        if soup.title and soup.title.string:
            meta_title = soup.title.string.strip()
        
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            meta_description = meta_desc['content'].strip()
    except:
        pass
    
    return url, meta_title, meta_description

def sync_seo_data():
    """从 Google Search Console 拉取数据并保存到数据库（按日期范围汇总）"""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    
    KEY_FILE_PATH = os.path.join(os.path.dirname(__file__), 'zhiyuanzhongyi-b17bb896700a.json')
    SITE_URL = 'sc-domain:baofengradio.co.uk'
    
    # 日期范围：今天到前3个月
    end_date = datetime.now() - timedelta(days=3)
    start_date = end_date - timedelta(days=90)
    
    print(f"📅 日期范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    
    try:
        print("🔗 连接 Google Search Console API...")
        creds = service_account.Credentials.from_service_account_file(
            KEY_FILE_PATH, scopes=['https://www.googleapis.com/auth/webmasters.readonly']
        )
        service = build('searchconsole', 'v1', credentials=creds)
        
        # 准备数据库
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 创建新表（带日期字段）
        cursor.execute('DROP TABLE IF EXISTS seo_pages')
        cursor.execute('''
            CREATE TABLE seo_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                clicks INTEGER DEFAULT 0,
                impressions INTEGER DEFAULT 0,
                ctr REAL DEFAULT 0,
                position REAL DEFAULT 0,
                meta_title TEXT,
                meta_description TEXT,
                start_date TEXT,
                end_date TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        
        # 获取整体数据
        request = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'dimensions': ['page'],
            'rowLimit': 1000
        }
        
        print("📊 正在获取搜索数据...")
        response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        rows = response.get('rows', [])
        print(f"✅ 获取到 {len(rows)} 条数据")
        
        # 准备数据
        pages_data = []
        for row in rows:
            pages_data.append({
                'url': row['keys'][0],
                'clicks': row.get('clicks', 0),
                'impressions': row.get('impressions', 0),
                'ctr': round(row.get('ctr', 0) * 100, 2),
                'position': round(row.get('position', 0), 1)
            })
        
        # 并行爬取 Meta 信息
        print("🚀 并行爬取 Meta 信息 (10 线程)...")
        urls = [p['url'] for p in pages_data]
        meta_map = {}
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_meta, url): url for url in urls}
            done = 0
            for future in as_completed(futures):
                url, title, desc = future.result()
                meta_map[url] = {'title': title, 'description': desc}
                done += 1
                if done % 50 == 0:
                    print(f"   已处理 {done}/{len(urls)}...")
        
        print(f"✅ Meta 信息爬取完成")
        
        # 保存数据（记录日期范围）
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        for page in pages_data:
            meta = meta_map.get(page['url'], {})
            cursor.execute('''
                INSERT INTO seo_pages (url, clicks, impressions, ctr, position, meta_title, meta_description, start_date, end_date, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (page['url'], page['clicks'], page['impressions'], page['ctr'], page['position'], 
                  meta.get('title', ''), meta.get('description', ''), start_str, end_str))
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 同步完成！共保存 {len(pages_data)} 条数据")
        print(f"   日期范围: {start_str} 至 {end_str}")
        
    except Exception as e:
        print(f"❌ 同步失败: {e}")
        raise

if __name__ == "__main__":
    sync_seo_data()
