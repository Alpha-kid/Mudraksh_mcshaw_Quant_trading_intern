# SELL
import yfinance as yf
from datetime import datetime
import json
from colorama import Fore, Back, Style
import requests
import numpy as np
import pandas as pd
tick=['TATASTEEL.NS','TATAPOWER.NS','TCS.NS','ADANIPORTS.NS','ADANIPOWER.NS','ADANIGREEN.NS','ADANIENT.NS','RELIANCE.NS','JSWSTEEL.NS','TATAMOTORS.NS']
import matplotlib.pyplot as plt
fg=pd.DataFrame()
for ipo in range(0,len(tick)):
  g=yf.download(tickers=tick[ipo], interval = '5m',start='2023-12-29',end='2024-01-29')
  from colorama import Fore, Back, Style
  kd=pd.DataFrame(g)
  pos=0
  prof=0
  loss=0
  ma=0
  stp=0
  target=0
  trn=0
  Quantity=0
  EntryTime=""
  Entryprice=""
  print('\n')
  # columns = ['Trade', 'EntryTime', 'EntryPrice', 'ExitPrice', 'ExitTime','Quantity','Pnl']

  columns=['Key','ExitTime','Symbol','EntryPrice','ExitPrice','Quantity','PositionStatus','Pnl','ExitType']
  df = pd.DataFrame(columns=columns)
  for i in range(4,len(kd)):
    if pos==0 and (kd['Close'][i]<((kd['High'][i]+kd['Low'][i])/2) and kd['Close'][i-1]<((kd['High'][i-1]+kd['Low'][i-1])/2) and kd['Close'][i-2]<((kd['High'][i-2]+kd['Low'][i-2])/2) ):
      pos=-1
      print('\n')
      print('Trade',trn)
      trn+=1
      trig=kd['Close'][i]
      EntryTime=str(kd.index[i])
      Entryprice=str(trig)
      print('EntryTime',kd.index[i])
      print('Entryprice',trig)
      # Quantity=int(20000/trig)
      Quantity=1
      ma=max(max(kd['High'][i],kd['High'][i-1]),kd['High'][i-2])
      stp=ma
      target=trig-3*(ma-trig)
    elif Quantity!=0 and pos==-1 and kd['High'][i]>stp :
      pos=0
      print('ExitPrice',stp)
      print('ExitTime',kd.index[i])
      print('*Loss',ma-trig)
      df.loc[len(df)]=[EntryTime,kd.index[i],tick[ipo],Entryprice,(stp),Quantity,-1,trig-ma,"Stoploss"]
      loss+=Quantity*(ma-trig)
    elif Quantity!=0 and pos==-1 and kd['Low'][i]<target:
      pos=0
      print('ExitPrice',target)
      print('ExitTime',kd.index[i])
      print('**Profit',trig-target)
      df.loc[len(df)]=[EntryTime,kd.index[i],tick[ipo],Entryprice,(target),Quantity,-1,trig-target,"Target"]
      prof+=Quantity*(trig-target)

  print(tick[ipo])
  print(Back.LIGHTGREEN_EX +str(prof))
  print(Back.LIGHTRED_EX +'-'+str(loss))


  Style.RESET_ALL
  if(prof-loss)>0:
    print('NetProfit',str(prof-loss))
  else:
    print('NetLoss',str(prof-loss))
  from colorama import init
  init(autoreset=True)
  df.to_csv(tick[ipo]+'Sell'+'.csv', index=False)
  print("----------------------------------------------------------------------------------------------------------")

# Buy
import yfinance as yf
from datetime import datetime
import json
from colorama import Fore, Back, Style
import requests
import numpy as np
import pandas as pd
tick=['TATASTEEL.NS','TATAPOWER.NS','TCS.NS','ADANIPORTS.NS','ADANIPOWER.NS','ADANIGREEN.NS','ADANIENT.NS','RELIANCE.NS','JSWSTEEL.NS','TATAMOTORS.NS']
df = pd.DataFrame(columns=columns)
import matplotlib.pyplot as plt
ipo=0

fg=pd.DataFrame()
for ipo in range(0,len(tick)):
  g=yf.download(tickers=tick[ipo], interval = '5m',start='2023-12-29',end='2024-01-29')
  kd=pd.DataFrame(g)
  pos=0
  prof=0
  loss=0
  ma=0
  stp=0
  target=0
  trn=0
  Quantity=0
  Entryprice=""
  EntryTime=""
  columns=['Key','ExitTime','Symbol','EntryPrice','ExitPrice','Quantity','PositionStatus','Pnl','ExitType']
  df = pd.DataFrame(columns=columns)
  print('\n')
  for i in range(4,len(kd)):
    if pos==0 and (kd['Close'][i]>((kd['High'][i]+kd['Low'][i])/2) and kd['Close'][i-1]>((kd['High'][i-1]+kd['Low'][i-1])/2) and kd['Close'][i-2]>((kd['High'][i-2]+kd['Low'][i-2])/2) ):
      pos=1
      trig=kd['Close'][i]
      print('\n')
      print('Trade',trn)
      trn+=1
      # Quantity=int(20000/trig)
      Quantity=1
      print('Quantity',int(20000/trig))
      print('EntryTime',kd.index[i])
      print('Entryprice',trig)
      EntryTime=str(kd.index[i])
      Entryprice=str(trig)
      ma=min(min(kd['Low'][i],kd['Low'][i-1]),kd['Low'][i-2])
      stp=ma
      target=trig+3*(trig-ma)
    elif Quantity!=0 and  pos==1 and kd['Low'][i]<stp :
      pos=0
      print('ExitPrice',stp)
      print('ExitTime',kd.index[i])
      print('*Loss',trig-stp)
      df.loc[len(df)]=[EntryTime,kd.index[i],tick[ipo],Entryprice,(stp),Quantity,1,stp-trig,"Stoploss"]
      loss+=Quantity*(trig-stp)
    elif Quantity!=0 and pos==1 and kd['High'][i]>target:
      pos=0
      print('ExitPrice',target)
      print('ExitTime',kd.index[i])
      print('**Profit',target-trig)
      df.loc[len(df)]=[EntryTime,kd.index[i],tick[ipo],Entryprice,(target),Quantity,1,target-trig,"target"]
      prof+=Quantity*(target-trig)

  print(tick[ipo])
  print(Back.LIGHTGREEN_EX +str(prof))
  print(Back.LIGHTRED_EX +'-'+str(loss))
  Style.RESET_ALL
  if(prof-loss)>0:
    print('NetProfit',str(prof-loss))
  else:
    print('NetLoss',str(prof-loss))
  df.to_csv(tick[ipo]+' '+'Buy'+'.csv', index=False)
  dirb="/content/"+tick[ipo]+" "+"Buy"+".csv"
  dirs="/content/"+tick[ipo]+"Sell"+".csv"
  df1 = pd.read_csv(dirb)
  df2 = pd.read_csv(dirs)
  merged_df = pd.concat([df1, df2], ignore_index=True)

  # Save the merged DataFrame to a new CSV file
  merged_df.to_csv("merged"+" "+tick[ipo]+".csv", index=False)