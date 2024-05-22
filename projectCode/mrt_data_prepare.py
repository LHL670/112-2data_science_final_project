import pandas as pd

# 讀取 CSV 檔案，指定分隔符號為逗號
df = pd.read_csv('./data/臺北捷運每日分時各站OD流量統計資料_202402.csv', on_bad_lines='skip')

# 建立 datetime 欄位
df['time'] = df['日期'] + ' ' + df['時段'].astype(str).str.strip().str.zfill(2) + ':00'
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M')

# 將 '人次' 轉換為整數
df['人次'] = df['人次'].astype(int)

# 依據 'time' 和 '進站' 分組並對 '人次' 進行求和，生成進站資料
df_in = df.groupby(['time', '進站']).agg({'人次': 'sum'}).reset_index()
df_in.rename(columns={'進站': '站點'}, inplace=True)
df_in['time'] = df_in['time'].dt.strftime('%Y-%m-%d %H:%M')
df_in.to_csv('./data/in.csv', index=False, encoding='utf-8-sig')

# 依據 'time' 和 '出站' 分組並對 '人次' 進行求和，生成出站資料
df_out = df.groupby(['time', '出站']).agg({'人次': 'sum'}).reset_index()
df_out.rename(columns={'出站': '站點'}, inplace=True)
df_out['time'] = df_out['time'].dt.strftime('%Y-%m-%d %H:%M')
df_out.to_csv('./data/out.csv', index=False, encoding='utf-8-sig')
