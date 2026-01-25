import sqlite3
import shutil
from datetime import datetime

# 1. 备份数据库
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
backup_path = f'ads_data_backup_{timestamp}.sqlite'
print(f"\n备份数据库到: {backup_path}")
shutil.copy2('ads_data.sqlite', backup_path)
print("备份完成!\n")

# 2. 连接数据库
conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

# 3. 分析并删除（使用大小写不敏感比较）
print("=" * 70)
print("数据清理统计（大小写不敏感）:")
print("=" * 70)

total_deleted = 0

# campaign表 - 使用 COLLATE NOCASE
cursor.execute("SELECT COUNT(*) FROM campaign WHERE LOWER(campaign_status) != 'enabled'")
count = cursor.fetchone()[0]  
print(f"campaign表 (campaign_status != 'Enabled'): {count} 条")
cursor.execute("DELETE FROM campaign WHERE LOWER(campaign_status) != 'enabled'")
total_deleted += cursor.rowcount

# asset表
cursor.execute("SELECT COUNT(*) FROM asset WHERE LOWER(status) != 'enabled'")
count = cursor.fetchone()[0]
print(f"asset表 (status != 'Enabled'): {count} 条")
cursor.execute("DELETE FROM asset WHERE LOWER(status) != 'enabled'")
total_deleted += cursor.rowcount

# channel表
cursor.execute("SELECT COUNT(*) FROM channel WHERE LOWER(status) != 'active'")
count = cursor.fetchone()[0]
print(f"channel表 (status != 'Active'): {count} 条")
cursor.execute("DELETE FROM channel WHERE LOWER(status) != 'active'")
total_deleted += cursor.rowcount

# search_term表 - Excluded 可能大小写混合
cursor.execute("SELECT COUNT(*) FROM search_term WHERE LOWER(added_excluded) = 'excluded'")
count = cursor.fetchone()[0]
print(f"search_term表 (added_excluded = 'Excluded'): {count} 条")
cursor.execute("DELETE FROM search_term WHERE LOWER(added_excluded) = 'excluded'")
total_deleted += cursor.rowcount

# product表 - Eligible
cursor.execute("SELECT COUNT(*) FROM product WHERE LOWER(status) != 'eligible'")
count = cursor.fetchone()[0]
print(f"product表 (status != 'Eligible'): {count} 条")
cursor.execute("DELETE FROM product WHERE LOWER(status) != 'eligible'")
total_deleted += cursor.rowcount

# audience表
cursor.execute("SELECT COUNT(*) FROM audience WHERE LOWER(segment_status) != 'enabled'")
count = cursor.fetchone()[0]
print(f"audience表 (segment_status != 'Enabled'): {count} 条")
cursor.execute("DELETE FROM audience WHERE LOWER(segment_status) != 'enabled'")
total_deleted += cursor.rowcount

# age表 - "Ad group paused" 需要精确匹配
cursor.execute("SELECT COUNT(*) FROM age WHERE status = 'Ad group paused'")
count = cursor.fetchone()[0]
print(f"age表 (status = 'Ad group paused'): {count} 条")
cursor.execute("DELETE FROM age WHERE status = 'Ad group paused'")
total_deleted += cursor.rowcount

# gender表
cursor.execute("SELECT COUNT(*) FROM gender WHERE status = 'Ad group paused'")
count = cursor.fetchone()[0]
print(f"gender表 (status = 'Ad group paused'): {count} 条")
cursor.execute("DELETE FROM gender WHERE status = 'Ad group paused'")
total_deleted += cursor.rowcount

print("=" * 70)
print(f"总计删除: {total_deleted} 条记录")
print("=" * 70)

# 4. 提交更改
conn.commit()
conn.close()

print(f"\n✅ 清理完成!")
print(f"📦 备份文件: {backup_path}\n")
