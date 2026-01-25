import sqlite3
import shutil
from datetime import datetime
import os

def backup_database():
    """备份数据库"""
    db_path = 'ads_data.sqlite'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f'ads_data_backup_{timestamp}.sqlite'
    
    print(f"📦 正在备份数据库到: {backup_path}")
    shutil.copy2(db_path, backup_path)
    print(f"✅ 备份完成！\n")
    return backup_path

def get_count(cursor, query):
    """获取查询结果数量"""
    cursor.execute(query)
    return cursor.fetchone()[0]

def analyze_cleanup(conn):
    """分析要清理的数据量"""
    cursor = conn.cursor()
    
    print("📊 分析要清理的数据量...\n")
    print("=" * 60)
    
    stats = {}
    
    # 1. campaign表
    query = "SELECT COUNT(*) FROM campaign WHERE campaign_status != 'enabled'"
    count = get_count(cursor, query)
    stats['campaign'] = count
    print(f"📌 campaign表: {count} 条记录 (campaign_status != 'enabled')")
    
    # 2. asset表
    query = "SELECT COUNT(*) FROM asset WHERE status != 'enabled'"
    count = get_count(cursor, query)
    stats['asset'] = count
    print(f"📌 asset表: {count} 条记录 (status != 'enabled')")
    
    # 3. channel表
    query = "SELECT COUNT(*) FROM channel WHERE status != 'active'"
    count = get_count(cursor, query)
    stats['channel'] = count
    print(f"📌 channel表: {count} 条记录 (status != 'active')")
    
    # 4. search_term表
    query = "SELECT COUNT(*) FROM search_term WHERE added_excluded = 'Excluded'"
    count = get_count(cursor, query)
    stats['search_term'] = count
    print(f"📌 search_term表: {count} 条记录 (added_excluded = 'Excluded')")
    
    # 5. product表
    query = "SELECT COUNT(*) FROM product WHERE status != 'Eligible'"
    count = get_count(cursor, query)
    stats['product'] = count
    print(f"📌 product表: {count} 条记录 (status != 'Eligible')")
    
    # 6. segment表
    query = "SELECT COUNT(*) FROM segment WHERE segment_status != 'enabled'"
    count = get_count(cursor, query)
    stats['segment'] = count
    print(f"📌 segment表: {count} 条记录 (segment_status != 'enabled')")
    
    # 7. audience表
    query = "SELECT COUNT(*) FROM audience WHERE segment_status != 'enabled'"
    count = get_count(cursor, query)
    stats['audience'] = count
    print(f"📌 audience表: {count} 条记录 (segment_status != 'enabled')")
    
    # 8. age表
    query = "SELECT COUNT(*) FROM age WHERE status = 'Ad group paused'"
    count = get_count(cursor, query)
    stats['age'] = count
    print(f"📌 age表: {count} 条记录 (status = 'Ad group paused')")
    
    # 9. gender表
    query = "SELECT COUNT(*) FROM gender WHERE status = 'Ad group paused'"
    count = get_count(cursor, query)
    stats['gender'] = count
    print(f"📌 gender表: {count} 条记录 (status = 'Ad group paused')")
    
    print("=" * 60)
    total = sum(stats.values())
    print(f"\n📊 总计: {total} 条记录将被删除\n")
    
    return stats, total

def execute_cleanup(conn):
    """执行清理操作"""
    cursor = conn.cursor()
    
    print("🗑️  开始执行清理...\n")
    
    deleted_counts = {}
    
    # 1. campaign表
    cursor.execute("DELETE FROM campaign WHERE campaign_status != 'enabled'")
    deleted_counts['campaign'] = cursor.rowcount
    print(f"✅ campaign表: 删除了 {cursor.rowcount} 条记录")
    
    # 2. asset表
    cursor.execute("DELETE FROM asset WHERE status != 'enabled'")
    deleted_counts['asset'] = cursor.rowcount
    print(f"✅ asset表: 删除了 {cursor.rowcount} 条记录")
    
    # 3. channel表
    cursor.execute("DELETE FROM channel WHERE status != 'active'")
    deleted_counts['channel'] = cursor.rowcount
    print(f"✅ channel表: 删除了 {cursor.rowcount} 条记录")
    
    # 4. search_term表
    cursor.execute("DELETE FROM search_term WHERE added_excluded = 'Excluded'")
    deleted_counts['search_term'] = cursor.rowcount
    print(f"✅ search_term表: 删除了 {cursor.rowcount} 条记录")
    
    # 5. product表
    cursor.execute("DELETE FROM product WHERE status != 'Eligible'")
    deleted_counts['product'] = cursor.rowcount
    print(f"✅ product表: 删除了 {cursor.rowcount} 条记录")
    
    # 6. segment表
    cursor.execute("DELETE FROM segment WHERE segment_status != 'enabled'")
    deleted_counts['segment'] = cursor.rowcount
    print(f"✅ segment表: 删除了 {cursor.rowcount} 条记录")
    
    # 7. audience表
    cursor.execute("DELETE FROM audience WHERE segment_status != 'enabled'")
    deleted_counts['audience'] = cursor.rowcount
    print(f"✅ audience表: 删除了 {cursor.rowcount} 条记录")
    
    # 8. age表
    cursor.execute("DELETE FROM age WHERE status = 'Ad group paused'")
    deleted_counts['age'] = cursor.rowcount
    print(f"✅ age表: 删除了 {cursor.rowcount} 条记录")
    
    # 9. gender表
    cursor.execute("DELETE FROM gender WHERE status = 'Ad group paused'")
    deleted_counts['gender'] = cursor.rowcount
    print(f"✅ gender表: 删除了 {cursor.rowcount} 条记录")
    
    conn.commit()
    
    print(f"\n✅ 清理完成！总计删除: {sum(deleted_counts.values())} 条记录")
    
    return deleted_counts

def main():
    print("\n" + "=" * 60)
    print("🧹 Google Ads 数据库清理工具")
    print("=" * 60 + "\n")
    
    # 1. 备份数据库
    backup_path = backup_database()
    
    # 2. 连接数据库
    conn = sqlite3.connect('ads_data.sqlite')
    
    try:
        # 3. 分析要清理的数据
        stats, total = analyze_cleanup(conn)
        
        # 4. 确认清理
        if total == 0:
            print("✨ 数据库已经很干净了，没有需要删除的记录！")
            return
        
        print(f"⚠️  警告: 即将删除 {total} 条记录！")
        print(f"📦 备份文件: {backup_path}\n")
        
        confirm = input("❓ 确认执行清理吗? (输入 'YES' 确认): ")
        
        if confirm == 'YES':
            # 5. 执行清理
            deleted_counts = execute_cleanup(conn)
            
            print("\n" + "=" * 60)
            print("🎉 数据库清理成功完成！")
            print("=" * 60)
        else:
            print("\n❌ 操作已取消")
    
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        print(f"💾 数据库已备份至: {backup_path}")
        conn.rollback()
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
