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

# 3. 分析并删除
print("=" * 70)
print("数据清理统计:")
print("=" * 70)

total_deleted = 0

# campaign表
cursor.execute("SELECT COUNT(*) FROM campaign WHERE campaign_status != 'enabled'")
count = cursor.fetchone()[0]
print(f"campaign表 (campaign_status != 'enabled'): {count} 条")
cursor.execute("DELETE FROM campaign WHERE campaign_status != 'enabled'")
total_deleted += cursor.rowcount

# asset表
cursor.execute("SELECT COUNT(*) FROM asset WHERE status != 'enabled'")
count = cursor.fetchone()[0]
print(f"asset表 (status != 'enabled'): {count} 条")
cursor.execute("DELETE FROM asset WHERE status != 'enabled'")
total_deleted += cursor.rowcount

# channel表
cursor.execute("SELECT COUNT(*) FROM channel WHERE status != 'active'")
count = cursor.fetchone()[0]
print(f"channel表 (status != 'active'): {count} 条")
cursor.execute("DELETE FROM channel WHERE status != 'active'")
total_deleted += cursor.rowcount

# search_term表
cursor.execute("SELECT COUNT(*) FROM search_term WHERE added_excluded = 'Excluded'")
count = cursor.fetchone()[0]
print(f"search_term表 (added_excluded = 'Excluded'): {count} 条")
cursor.execute("DELETE FROM search_term WHERE added_excluded = 'Excluded'")
total_deleted += cursor.rowcount

# product表
cursor.execute("SELECT COUNT(*) FROM product WHERE status != 'Eligible'")
count = cursor.fetchone()[0]
print(f"product表 (status != 'Eligible'): {count} 条")
cursor.execute("DELETE FROM product WHERE status != 'Eligible'")
total_deleted += cursor.rowcount

# audience表
cursor.execute("SELECT COUNT(*) FROM audience WHERE segment_status != 'enabled'")
count = cursor.fetchone()[0]
print(f"audience表 (segment_status != 'enabled'): {count} 条")
cursor.execute("DELETE FROM audience WHERE segment_status != 'enabled'")
total_deleted += cursor.rowcount

# age表
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
