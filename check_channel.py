import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
c = conn.cursor()

# 检查 channel 表
count = c.execute('SELECT COUNT(*) FROM channel').fetchone()[0]
print(f'Channel表记录数: {count}')

if count > 0:
    # 查看示例数据
    rows = c.execute('SELECT * FROM channel LIMIT 3').fetchall()
    print(f'\n前3条记录:')
    for row in rows:
        print(row)
else:
    print('\n⚠️ Channel表是空的！')

# 检查其他表
tables = ['campaign', 'asset', 'product', 'search_term', 'audience']
print('\n其他表记录数:')
for table in tables:
    count = c.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
    print(f'{table}: {count} 条')

conn.close()
