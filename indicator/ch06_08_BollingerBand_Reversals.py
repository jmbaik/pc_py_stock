import matplotlib.pyplot as plt
from Investar import Analyzer

mk = Analyzer.MarketDB()
df = mk.get_daily_price('SK하이닉스', '20181101')
  
df['MA20'] =  df['CLOSE'].rolling(window=20).mean() 
df['stddev'] =  df['CLOSE'].rolling(window=20).std() 
df['upper'] = df['MA20'] + (df['stddev'] * 2)
df['lower'] = df['MA20'] - (df['stddev'] * 2)
df['PB'] = ( df['CLOSE'] - df['lower']) / (df['upper'] - df['lower'])

df['II'] = (2* df['CLOSE']-df['HIGH']-df['LOW'])/(df['HIGH']-df['LOW'])*df['VOL']
df['IIP21'] = df['II'].rolling(window=21).sum()/df['VOL'].rolling(window=21).sum()*100
df = df.dropna()

plt.figure(figsize=(9, 9))
plt.subplot(3, 1, 1)
plt.title('SK Hynix Bollinger Band(20 day, 2 std) - Reversals')
plt.plot(df.index,  df['CLOSE'], 'm', label='Close')
plt.plot(df.index, df['upper'], 'r--', label ='Upper band')
plt.plot(df.index, df['MA20'], 'k--', label='Moving average 20')
plt.plot(df.index, df['lower'], 'c--', label ='Lower band')
plt.fill_between(df.index, df['upper'], df['lower'], color='0.9')
for i in range(0, len(df.CLOSE)):
    if df.PB.values[i] < 0.05 and df.IIP21.values[i] > 0:   
        plt.plot(df.index.values[i], df.CLOSE.values[i], 'r^')
    elif df.PB.values[i] > 0.95 and df.IIP21.values[i] < 0: 
        plt.plot(df.index.values[i], df.CLOSE.values[i], 'bv')


plt.legend(loc='best')
plt.subplot(3, 1, 2)
plt.plot(df.index, df['PB'], 'b', label='%b')
plt.grid(True)
plt.legend(loc='best')

plt.subplot(3, 1, 3)
