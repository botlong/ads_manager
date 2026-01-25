import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

print("\n" + "=" * 70)
print("检查数据库中的实际值（大小写）")
print("=" * 70 + "\n")

# 1. campaign表 - campaign_status
cursor.execute("SELECT DISTINCT campaign_status FROM campaign")
print("📌 campaign.campaign_status 的实际值:")
for row in cursor.fetchall():
    print(f"   '{row[0]}'")

# 2. asset表 - status
cursor.execute("SELECT DISTINCT status FROM asset")
print("\n📌 asset.status 的实际值:")
for row in cursor.fetchall():
    print(f"   '{row[0]}'")

# 3. channel表 - status
cursor.execute("SELECT DISTINCT status FROM channel")
print("\n📌 channel.status 的实际值:")
for row in cursor.fetchall():
    print(f"   '{row[0]}'")

# 4. search_term表 - added_excluded
cursor.execute("SELECT DISTINCT added_excluded FROM search_term")
print("\n📌 search_term.added_excluded 的实际值:")
for row in cursor.fetchall():
    print(f"   '{row[0]}'")

# 5. product表 - status
cursor.execute("SELECT DISTINCT status FROM product")
print("\n📌 product.status 的实际值:")
for row in cursor.fetchall():
    print(f"   '{row[0]}'")

# 6. audience表 - segment_status
cursor.execute("SELECT DISTINCT segment_status FROM audience")
print("\n📌 audience.segment_status 的实际值:")
for row in cursor.fetchall():
    print(f"   '{row[0]}'")

# 7. age表 - status
cursor.execute("SELECT DISTINCT status FROM age LIMIT 10")
print("\n📌 age.status 的实际值（前10个）:")
for row in cursor.fetchall():
    print(f"   '{row[0]}'")

# 8. gender表 - status
cursor.execute("SELECT DISTINCT status FROM gender LIMIT 10")
print("\n📌 gender.status 的实际值（前10个）:")
for row in cursor.fetchall():
    print(f"   '{row[0]}'")

print("\n" + "=" * 70 + "\n")
conn.close()
