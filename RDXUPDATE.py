import logging
import numpy as np
import talib as ta
from expiry import getExpiryData
from datetime import datetime, time
from algoLogic import optOverNightAlgoLogic
from histData import getFnoHistData, getFnoBacktestData
import backTestTools


# Define a class algoLogic that inherits from optOverNightAlgoLogic
class algoLogic(optOverNightAlgoLogic):

    # Define a method to get current expiry epoch
    def getCurrentExpiryEpoch(self, date, baseSym):
        # Fetch expiry data for current and next expiry
        expiryData = getExpiryData(date, baseSym)
        nextExpiryData = getExpiryData(date+86400, baseSym)

        # Select appropriate expiry based on the current date
        expiry = expiryData["CurrentExpiry"]
        expiryDatetime = datetime.strptime(expiry, "%d%b%y")

        if self.humanTime.date() == expiryDatetime.date():
            expiry = nextExpiryData["CurrentExpiry"]
        else:
            expiry = expiryData["CurrentExpiry"]

        # Set expiry time to 15:20 and convert to epoch
        expiryDatetime = datetime.strptime(expiry, "%d%b%y")
        expiryDatetime = expiryDatetime.replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()

        return expiryEpoch

    # Define a method to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):

        # Add necessary columns to the DataFrame
        col = ["Target", "Stoploss", "BaseSymStoploss", "Expiry","trailing"]
        self.addColumnsToOpenPnlDf(col)

        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_5min = getFnoBacktestData(
                indexSym, startEpoch-432000, endEpoch, "5Min")
            df_Daily=getFnoBacktestData(indexSym,startEpoch-2160000,endEpoch,"1D")
            
        except Exception as e:
            # Log an exception if data retrieval fails
            logging.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Drop rows with missing values
        df.dropna(inplace=True)
        df_5min.dropna(inplace=True)
        df_Daily.dropna(inplace=True)

        # Calculate RSI indicator
        df_5min["rsi"] = ta.RSI(df_5min["c"], timeperiod=14)
        df_5min.dropna(inplace=True)
        df_Daily["Emah"] = ta.EMA(df_Daily["h"], timeperiod=5)
        df_Daily.dropna(inplace=True)
        df_Daily["Emal"] = ta.EMA(df_Daily["l"], timeperiod=5)
        df_Daily.dropna(inplace=True)
        # Filter dataframe from timestamp greater than start time timestamp
        df_5min = df_5min[df_5min.index > startEpoch]
        df_Daily = df_Daily[df_Daily.index > startEpoch]

        # Determine crossover signals
        df_5min["rsiCross60"] = np.where(
            (df_5min["rsi"] > 60) & (df_5min["rsi"].shift(1) <= 60), 1, 0)
        df_5min["rsiCross40"] = np.where(
            (df_5min["rsi"] < 40) & (df_5min["rsi"].shift(1) >= 40), 1, 0)
        df_5min["rsiCross50"] = np.where((df_5min["rsi"] >= 50) & (df_5min["rsi"].shift(
            1) < 50), 1, np.where((df_5min["rsi"] <= 50) & (df_5min["rsi"].shift(1) > 50), 1, 0))
        df_Daily["EmahCond"] = np.where((df_Daily["c"] >= df_Daily['Emah']) , 1, 0)
        df_Daily["EmalCond"]=np.where((df_Daily["c"] <= df_Daily['Emal']) , 1, 0)

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_5min.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv")

        # Strategy Parameters
        putc=0
        callc=0
        Trade=True
        prevt=[]
        for timeData in df.index[:3]:
            prevt.append(timeData)
        

        # Loop through each timestamp in the DataFrame index
        for timeData in df.index[3:]:
            # Reset tradeCounter on new day
            

            self.timeData = float(timeData)
            # humanTime =datetime.datetime.fromtimestamp(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)
            for index, row in self.openPnl.iterrows():
                try:
                    data = getFnoHistData(
                        row['Symbol'], timeData)
                    self.openPnl.at[index, 'CurrentPrice'] = data['c']
                except Exception as e:
                    logging.info(e)
            self.pnlCalculator()
            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            # Update lastIndexTimeData
            
            timestamp=timeData

            # Strategy Specific Trading Time
            

            # Log relevant information
            if(timestamp in df_5min.index and timestamp!=datetime.time(9,30)):
                prevt.pop(0)
                prevt.append(timeData)
            logging.info(f'-----time=>--Time {self.humanTime}---------------')
            time_component = self.humanTime.time()
            time_to_compare1 = datetime.time(15, 30)
            time_to_compare2 = datetime.time(9, 15)
            if(time_component<=time_to_compare1 and time_component>=time_to_compare2):
                Timenow=timestamp
                data = getFnoHistData(indexName,timeData)
                ope=data['o']
                high=data['h']
                low=data['l']
                close=data['c']
                logging.info(f'o=>-{ope}--h=>-{high}-l=>-{low}-c=>-{close}---')
                if(timestamp in df_5min.index):
                    logging.info(df_5min['RSI'][timestamp])
                    
                    if(len(prevt)>=3):
                        timesprev=prevt[len(prevt)-2]
                        timesprev2=prevt[len(prevt)-3]
                        logging.info(f'Rsi__TRUE---{timestamp}')
                        symWithExpiry=baseSym+backTestTools.getCurrentExpiry(timestamp)
                        if(df_5min['RSI'][timestamp]<50 and df_5min['RSI'][timesprev]>=50) or (df_5min['RSI'][timestamp]>50 and df_5min['RSI'][timesprev]<=50):
                            Trade=True
                            # logging.info(f'Rsi__TRUE---{timestamp}')
                        #exitconditions->
                        if not self.openPnl.empty:
                            for index,row in self.openPnl.iterrows():
                                sym = row["Symbol"]
                                sym = sym[len(sym)-2:]
                                if sym=='CE'  and df_5min['h'][timestamp]>=row['Slfixed'] :
                                    self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timestamp)] + [row['Symbol']] +\
                                        [row['EntryPrice']] +\
                                        [row['CurrentPrice']] +\
                                        [row['PositionStatus']*row['Quantity']] +\
                                        [0]+["HighHit"] 
                                    if(sym=='PE'):
                                        putc-=1
                                    else:
                                        callc-=1
                                    self.openPnl.drop(index, inplace=True)
                                elif(sym=='PE' and df_5min['l'][timestamp]<=row['Slfixed']):
                                    self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timestamp)] + [row['Symbol']] +\
                                        [row['EntryPrice']] +\
                                        [row['CurrentPrice']] +\
                                        [row['PositionStatus']*row['Quantity']] +\
                                        [0]+["LowHit"]
                                    if(sym=='PE'):
                                        putc-=1
                                    else:
                                        callc-=1
                                    self.openPnl.drop(index, inplace=True)
                            self.openPnl.reset_index(drop=True, inplace=True)
                        # entryconditions->
                        if (putc<3 and callc<3 and backTestTools.getCurrentExpiry(timestamp))!=backTestTools.getCurrentExpiry(timestamp+86400) and df_5min['RSI'][timesprev2]<=60  and Trade==True  and df_5min['RSI'][timesprev]>60:
                            Trade=False
                            print(1)
                            
                        
                            indexData=backTestTools.getHistData1Min(timestamp=timestamp,
                                                                    symbol=indexName,
                                                                    conn=self.conn)
                            symWithExpiry=baseSym+backTestTools.getCurrentExpiry(timestamp+86400)

                            putSym = self.getPut(indexPrice=indexData['c'], strikeDist=strikeDist,
                                                symWithExpiry=symWithExpiry, otmFactor=0)

                            putData = backTestTools.getHistData1Min(
                                timestamp=timestamp, symbol=putSym, conn=self.conn)
                            
                            logging.info(f'Data for {putSym} is {putData}')
                            sl=min(df_5min['l'][timesprev],df_5min['l'][timesprev2])

                            putPrice = self.entryOrder(data=putData, symbol=putSym,
                                                    quantity=quantity, entrySide='SELL',sl=sl)
                            logging.info(f'PuTsell {putPrice}')
                            putc+=1
                            
                        elif (callc<3 and putc<3 and backTestTools.getCurrentExpiry(timestamp))!=backTestTools.getCurrentExpiry(timestamp+86400) and Trade==True  and df_5min['RSI'][timesprev]<40 and df_5min['RSI'][timesprev2]>=40 :
                            print(1)
                            try:
                                Trade=False
                                indexData=backTestTools.getHistData1Min(timestamp=timestamp,
                                                                        symbol=indexName,
                                                                        conn=self.conn)
                                symWithExpiry=baseSym+backTestTools.getCurrentExpiry(timestamp+86400)
                                callSym = self.getCall(indexPrice=indexData['c'], strikeDist=strikeDist,
                                                    symWithExpiry=symWithExpiry, otmFactor=0)
                                

                                # Fetch option price 1 min candle data from the database

                                callData = backTestTools.getHistData1Min(
                                    timestamp=timestamp, symbol=callSym, conn=self.conn)
                                
                                logging.info(f'Data for {callSym} is {callData}')
                                sl=max(df_5min['h'][timesprev],df_5min['h'][timesprev2])

                                callPrice = self.entryOrder(data=callData, symbol=callSym,
                                                        quantity=quantity, entrySide='SELL',sl=sl)
                                logging.info(f'Callsell {callPrice}')
                                callc+=1
                            except Exception as e:
                                logging.info(e)
                                continue
                        elif putc<3 and callc<3 and Trade==True  and df_5min['RSI'][timesprev]>60 and df_5min['RSI'][timesprev2]<=60 :
                            
                            
                            Trade=False
                            indexData=backTestTools.getHistData1Min(timestamp=timestamp,
                                                                    symbol=indexName,
                                                                    conn=self.conn)

                            putSym = self.getPut(indexPrice=indexData['c'], strikeDist=strikeDist,
                                                symWithExpiry=symWithExpiry, otmFactor=0)

                            putData = backTestTools.getHistData1Min(
                                timestamp=timestamp, symbol=putSym, conn=self.conn)
                            
                            logging.info(f'Data for {putSym} is {putData}')
                            
                            sl=min(df_5min['l'][timesprev],df_5min['l'][timesprev2])

                            putPrice = self.entryOrder(data=putData, symbol=putSym,
                                                    quantity=quantity, entrySide='SELL',sl=sl)
                            logging.info(f'PuTsell {putPrice}')
                            putc+=1
                        
                        elif callc<3 and putc<3 and Trade==True  and df_5min['RSI'][timesprev]<40 and df_5min['RSI'][timesprev2]>=40 :
                            
                            try:
                                Trade=False
                                indexData=backTestTools.getHistData1Min(timestamp=timestamp,
                                                                        symbol=indexName,
                                                                        conn=self.conn)
                                callSym = self.getCall(indexPrice=indexData['c'], strikeDist=strikeDist,
                                                    symWithExpiry=symWithExpiry, otmFactor=0)

                                # Fetch option price 1 min candle data from the database

                                callData = backTestTools.getHistData1Min(
                                    timestamp=timestamp, symbol=callSym, conn=self.conn)
                                
                                logging.info(f'Data for {callSym} is {callData}')
                                sl=max(df_5min['h'][timesprev],df_5min['h'][timesprev2])

                                callPrice = self.entryOrder(data=callData, symbol=callSym,
                                                        quantity=quantity, entrySide='SELL',sl=sl)
                                logging.info(f'Callsell {callPrice}')
                                callc+=1
                            except Exception as e:
                                logging.info(e)
                                continue
                        
                        prevt.pop(0)
                # exitcondition1m->
                Timenow=timestamp
                if not self.openPnl.empty:
                    time_component = humanTime.time()
                    time_to_compare1 = datetime.time(15, 15)
                    for index,row in self.openPnl.iterrows():
                        sym = row["Symbol"]
                        sym = sym[len(sym)-2:]

                        if row['PositionStatus']==-1 and row['CurrentPrice']>=row['EntryPrice']*(1+lossValuePercent):
                            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(Timenow)] + [row['Symbol']] +\
                                [row['EntryPrice']] +\
                                [row['CurrentPrice']] +\
                                [row['PositionStatus']*row['Quantity']] +\
                                [0]+["Losshit"]
                            if(sym=='PE'):
                                putc-=1
                            else:
                                callc-=1
                            self.openPnl.drop(index, inplace=True)
                        elif row['PositionStatus']==-1 and row['CurrentPrice']<=row['EntryPrice']*(1-rewardPercent):
                            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(Timenow)] + [row['Symbol']] +\
                                [row['EntryPrice']] +\
                                [row['CurrentPrice']] +\
                                [row['PositionStatus']*row['Quantity']] +\
                                [0]+["TargetHit"]
                            if(sym=='PE'):
                                putc-=1
                            else:
                                callc-=1
                            self.openPnl.drop(index, inplace=True)
                        
                        
                        elif time_component>=time_to_compare1 and (row['Symbol'][6:13])==datetime.datetime.fromtimestamp(timestamp).date().strftime('%d%b%y').upper():
                            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timestamp)] + [row['Symbol']] +\
                                [row['EntryPrice']] +\
                                [row['CurrentPrice']] +\
                                [row['PositionStatus']*row['Quantity']] +\
                                [0]+["TimeUP"]
                            if(sym=='PE'):
                                putc-=1
                            else:
                                callc-=1
                            self.openPnl.drop(index, inplace=True)

                    
                    self.openPnl.reset_index(drop=True, inplace=True)
                    self.pnlCalculator()
        self.combinePnlCsv()


if __name__ == "__main__":
    # Define Strategy Nomenclature
    devName = "SA"
    strategyName = "RDX_UpDate"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2021, 1, 1, 9, 15)
    endDate = datetime(2021, 1, 10, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = 'NIFTY'
    indexName = 'NIFTY 50'

    # Execute the algorithm
    algo.run(startDate, endDate, baseSym, indexName)
