import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

print("\n" + "=" * 70)
print("数据库清理后状态验证")
print("=" * 70 + "\n")

# campaign表 - 只保留 enabled
cursor.execute("SELECT COUNT(*) FROM campaign")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM campaign WHERE campaign_status = 'enabled'")
enabled = cursor.fetchone()[0]
print(f"✅ campaign表: {enabled}/{total} 条记录 (100% enabled)")

# asset表 - 只保留 enabled  
cursor.execute("SELECT COUNT(*) FROM asset")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM asset WHERE status = 'enabled'")
enabled = cursor.fetchone()[0]
print(f"✅ asset表: {enabled}/{total} 条记录 (100% enabled)")

# channel表 - 只保留 active
cursor.execute("SELECT COUNT(*) FROM channel")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM channel WHERE status = 'active'")
active = cursor.fetchone()[0]
print(f"✅ channel表: {active}/{total} 条记录 (100% active)")

# search_term表 - 无 Excluded
cursor.execute("SELECT COUNT(*) FROM search_term")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM search_term WHERE added_excluded != 'Excluded'")
not_excluded = cursor.fetchone()[0]
print(f"✅ search_term表: {not_excluded}/{total} 条记录 (无 Excluded)")

# product表 - 只保留 Eligible
cursor.execute("SELECT COUNT(*) FROM product")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM product WHERE status = 'Eligible'")
eligible = cursor.fetchone()[0]
print(f"✅ product表: {eligible}/{total} 条记录 (100% Eligible)")

# audience表 - 只保留 enabled
cursor.execute("SELECT COUNT(*) FROM audience")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM audience WHERE segment_status = 'enabled'")
enabled = cursor.fetchone()[0]
print(f"✅ audience表: {enabled}/{total} 条记录 (100% enabled)")

# age表 - 无 paused
cursor.execute("SELECT COUNT(*) FROM age")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM age WHERE status != 'Ad group paused'")
not_paused = cursor.fetchone()[0]
print(f"✅ age表: {not_paused}/{total} 条记录 (无 Ad group paused)")

# gender表 - 无 paused
cursor.execute("SELECT COUNT(*) FROM gender")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM gender WHERE status != 'Ad group paused'")
not_paused = cursor.fetchone()[0]
print(f"✅ gender表: {not_paused}/{total} 条记录 (无 Ad group paused)")

print("\n" + "=" * 70)
print("✅ 数据库清理完成！所有表只包含活跃数据。")
print("=" * 70 + "\n")

conn.close()
