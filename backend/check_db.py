import sqlite3
conn = sqlite3.connect('reddit_dashboard.db')
rows = conn.execute(
    'SELECT created_date, COUNT(*) FROM reddit_post_facts GROUP BY created_date ORDER BY created_date ASC'
).fetchall()
print(f'Total dates: {len(rows)}')
print('--- First 10 ---')
for r in rows[:10]:
    print(r)
print('--- Last 10 ---')
for r in rows[-10:]:
    print(r)
total = conn.execute('SELECT COUNT(*) FROM reddit_post_facts').fetchone()
print(f'\nTotal rows: {total[0]}')
post_types = conn.execute(
    'SELECT post_type, COUNT(*) FROM reddit_post_facts GROUP BY post_type ORDER BY COUNT(*) DESC'
).fetchall()
print('\nPost types:')
for r in post_types:
    print(r)
conn.close()
