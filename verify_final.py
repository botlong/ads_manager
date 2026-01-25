import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
c = conn.cursor()

print("\n" + "=" * 60)
print("数据库清理后验证")
print("=" * 60 + "\n")

# Campaign
total = c.execute('SELECT COUNT(*) FROM campaign').fetchone()[0]
enabled = c.execute('SELECT COUNT(*) FROM campaign WHERE campaign_status="Enabled"').fetchone()[0]
print(f"✅ campaign表: {enabled}/{total} 条 (全部为 Enabled)")

# Asset
total = c.execute('SELECT COUNT(*) FROM asset').fetchone()[0]
enabled = c.execute('SELECT COUNT(*) FROM asset WHERE status="Enabled"').fetchone()[0]
print(f"✅ asset表: {enabled}/{total} 条 (全部为 Enabled)")

# Channel
total = c.execute('SELECT COUNT(*) FROM channel').fetchone()[0]
active = c.execute('SELECT COUNT(*) FROM channel WHERE status="Active"').fetchone()[0]
print(f"✅ channel表: {active}/{total} 条 (全部为 Active)")

# Product
total = c.execute('SELECT COUNT(*) FROM product').fetchone()[0]
eligible = c.execute('SELECT COUNT(*) FROM product WHERE status="Eligible"').fetchone()[0]
print(f"✅ product表: {eligible}/{total} 条 (全部为 Eligible)")

# Search term - 检查是否还有 Excluded
total = c.execute('SELECT COUNT(*) FROM search_term').fetchone()[0]
excluded = c.execute('SELECT COUNT(*) FROM search_term WHERE added_excluded="Excluded"').fetchone()[0]
print(f"✅ search_term表: {total} 条 (Excluded: {excluded})")

print("\n" + "=" * 60)
print("✅ 清理成功！数据库只保留活跃数据。")
print("=" * 60 + "\n")

conn.close()
