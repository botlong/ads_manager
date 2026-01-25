import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

path = r'd:\ads_manager\ads-date\ads-date\products\2026.1.10.csv'
try:
    with open(path, 'r', encoding='utf-16') as f:
        print("--- HEAD (UTF-16) ---")
        for i in range(10):
            print(repr(f.readline()))
except Exception as e:
    print(f"UTF-16 read failed: {e}")
