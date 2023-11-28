import requests, json, pandas as pd, matplotlib.pyplot as plt
"""
api1 = 'https://financialmodelingprep.com/api/v3/historical-price-full/USDIDR?to=2023-11-07&apikey=PdK9QjSbplzsQY1TkBc0Pmv3XZ1YyXtd'
api2 = 'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol=IDR&outputsize=full&apikey=5C5XMUDURIAR4OCU'

#Panggil API Pertama dan save data kepada data1.json
r = requests.get(api1)
data = r.json()
with open('data1.json', 'w') as f:
    json.dump(data, f)

#Panggil API Kedua dan save data kepada data2.json
r = requests.get(api2)
data = r.json()
with open('data2.json', 'w') as f:
    json.dump(data, f)
"""
#Buka data1.json lalu ekstrak, load dan tampilkan
with open("data1.json") as file:
    data = json.load(file)
data = pd.read_json("data.json",orient="columns")
df1 = pd.json_normalize(data['historical'])
print(df1[['date','close']])

#Buka data2.json lalu ekstrak, load dan tampilkan
with open("data2.json") as file:
    json_data = json.load(file)
fx_data = json_data['Time Series FX (Daily)']
df2 = pd.DataFrame.from_dict(fx_data, orient='index')
df2.reset_index(inplace=True)
df2.rename(columns={'index': 'date', '4. close': 'close'}, inplace=True)
df2['close'] = df2['close'].astype(float)
print(df2[['date', 'close']])

#Menggabungkan kedua dataframe menjadi satu
df = pd.merge(df1, df2, on='date', how='inner')
print(df[['date', 'close_x', 'close_y']])

#Index/Date diubah menjadi data type datetime
df['date'] = pd.to_datetime(df['date'])

#Menggambarkan diagram menggunakan dataframe yang telah digabungkan
plt.figure(figsize=(10, 5))
plt.plot(df['date'], df['close_x'], marker='o')
plt.plot(df['date'], df['close_y'], marker='o')
plt.title('USD to IDR')
plt.xlabel('Date')
plt.ylabel('Value')
plt.grid(True)
plt.tight_layout()
plt.show()