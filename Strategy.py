from pymongo import MongoClient
import logging
import pandas as pd
import backTestTools  # used to fetch historic data and historic option expiries
import datetime
# from dataLogger import logData  # Used for logging strategy outputs


class algoLogic:

    '''Global variable declaration'''

    conn = None
    timeData = None

    # Save the results of the backtest in this folder
    writeFileLocation = r'./BackTestResults/'

    '''Save the info of the open position in the below df'''

    openPnl = pd.DataFrame(
        columns=['Key', 'Symbol', 'EntryPrice', 'CurrentPrice',
                 'Quantity',
                 'PositionStatus', 'Pnl'])

    '''Save the info of close position in the below df'''

    closedPnl = pd.DataFrame(
        columns=['Key', 'ExitTime', 'Symbol', 'EntryPrice', 'ExitPrice',
                 'Quantity', 'Pnl', 'ExitType'])

    def connectToMongo(self, userName, password):
        '''Used for connecting to the mongoDb instance'''

        try:
            self.conn = MongoClient(
                host="139.5.189.42", port=27017, username=userName, password=password)
            logging.info("Connected successfully to MongoDB!!!")
        except Exception as e:
            raise Exception(e)

    def getCall(self, indexPrice, strikeDist,
                symWithExpiry, otmFactor):
        '''Used to get the call symbol'''

        '''
            Inputs required are -
            indexPrice - Price of the underlying index
            srikeDist - Distance between the consecutive strikes
            symWithExpiry - baseString of the option symbol. Example: NIFTY09SEP21, BANKNIFTY07OCT21, etc
            otmFactor - 0 to get at the money(ATM) call, 1xstrikeDist(i.e. 50 for NIFTY, 100 for BANKNIFTY),
            (-1)xstrikeDist(i.e. -50 for NIFTY, -100 for BANKNIFTY) for in the money (ITM) call
        '''
        remainder = indexPrice % strikeDist
        atm = indexPrice - remainder if remainder <= (strikeDist/2) \
            else (indexPrice - remainder + strikeDist)
        callSym = symWithExpiry+str(int(atm)+otmFactor) + 'CE'
        return callSym

    def getPut(self, indexPrice, strikeDist,
               symWithExpiry, otmFactor):
        '''Used to get the put symbol'''

        '''
            Inputs required are -
            indexPrice - Price of the underlying index
            srikeDist - Distance between the consecutive strikes
            symWithExpiry - baseString of the option symbol. Example: NIFTY09SEP21, BANKNIFTY07OCT21, etc
            otmFactor - 0 to get at the money(ATM) put, 1xstrikeDist(i.e. 50 for NIFTY, 100 for BANKNIFTY),
            (-1)xstrikeDist(i.e. -50 for NIFTY, -100 for BANKNIFTY) for in the money (ITM) put
        '''

        remainder = indexPrice % strikeDist
        atm = indexPrice - remainder if remainder <= (strikeDist/2) \
            else (indexPrice - remainder + strikeDist)
        putSym = symWithExpiry+str(int(atm)-otmFactor) + 'PE'
        return putSym

    def entryOrder(self, data, symbol, quantity, entrySide):
        '''Used to update the new position in the openPnl Df'''

        entryPrice = data['c']

        self.currentKey = self.timeData

        positionSide = 1 if entrySide == 'BUY' else -1

        '''Adding position to the openPnl df using levelAdder method'''

        self.levelAdder(entryPrice, symbol, quantity, positionSide)

        return entryPrice

    def pnlCalculator(self):
        '''Calculting the pnl for open and closed positions and saving it to the relevant .csv files'''

        if not self.openPnl.empty:
            self.openPnl['Pnl'] = (self.openPnl['CurrentPrice']-self.openPnl['EntryPrice']) * \
                self.openPnl['Quantity']*self.openPnl['PositionStatus']
            try:
                self.openPnl.to_csv(self.writeFileLocation +
                                    'openPosition.csv' % ())
            except:
                pass

        if not self.closedPnl.empty:
            self.closedPnl['Pnl'] = (
                self.closedPnl['ExitPrice']-self.closedPnl['EntryPrice'])*self.closedPnl['Quantity']
            try:
                self.closedPnl.to_csv(
                    self.writeFileLocation+'closePosition.csv' % ())
            except:
                pass

    def levelAdder(self, entryPrice, symbol, quantity, positionSide):
        '''Appending the new positon to the openPnl df and saving it to the relevant file'''

        self.openPnl.loc[len(self.openPnl)] = [datetime.datetime.fromtimestamp(self.timeData)]+[symbol] + [entryPrice] \
            + [entryPrice] + [quantity]+[positionSide] + [0]

        try:
            self.openPnl.to_csv(self.writeFileLocation + 'openPosition.csv')
        except:
            pass

    def run(self, startDateTime, endDateTime,
            strikeDist, symWithExpiry,
            indexName, lossValuePercent, rewardPercent,
            quantity, userName, password):
        '''Establish connection to the mongoDB server '''

        self.connectToMongo(userName, password)

        '''--------------------------------------------------------------------------'''

        '''
            Create a log file for saving all the information generated in the strategy

            NOTE -  Try to create a very detail log file as it greatly helps in the testing, debuging and
                    logic verification process.

                    NEVER MAKE A STRATEGY WITHOUT A LOG FILE!!!

                    Log files to be saved in the strategy Log folder

                    Format for log files -

                    TypeOfLogFile_UnderLyingName_StartDate_AnyAdditionalRelevantInfo_logFile.log

            Example -
                    SignalMaker_NIFTY 50_2021-01-01 09:15:00_logfile.log
                    ExecutionLogic_NIFTY 50_2021-01-01 09:15:00_logfile.log

        '''
        pnlDate = str(startDateTime)
        pnlDate += indexName
        self.writeFileLocation += pnlDate   
        logFileName = f'StrategyLog/ExecutionLog_{indexName}_{startDateTime}_logfile.log'
        logFileName = logFileName.replace(':', '')

        # Delete any old log files of the same name
        f = open(logFileName, 'w')
        f.close()

        # Delete any old log files of the same name
        # f = open(logFileName, 'w')
        # f.close()

        # Set the logging level as per requirement. For Development and testing preferred level is DEBUG
        # Don't change the format of the log file
        # filename = ''

        logging.basicConfig(level=logging.DEBUG,
                            filename=logFileName, filemode='w', force=True)

        logging.info(
            '\n-----------------------------New start-----------------------\n')

        '''--------------------------------------------------------------------------'''

        # Coverting datetime objects to epoch
        startTimeEpoch = int(datetime.datetime(startDateTime.year, startDateTime.month,
                             startDateTime.day, startDateTime.hour, startDateTime.minute, 0).timestamp())
        endTimeEpoch = int(datetime.datetime(endDateTime.year, endDateTime.month,
                           endDateTime.day, endDateTime.hour, endDateTime.minute, 0).timestamp())

        '''--------------------------------------------------------------------------'''
        DataFrame_day = backTestTools.getBackTestData1Min(
            startDateTime=startTimeEpoch,endDateTime=endTimeEpoch, symbol=indexName, conn=self.conn)
        DataFrame_day.set_index('ti',inplace=True)
        # print(DataFrame_day)
        Trade=True
        for timestamp in DataFrame_day.index:
            self.timeData = timestamp
            humanTime =datetime.datetime.fromtimestamp(timestamp)
            logging.info(f'------------Time {humanTime}---------------')
            for index, row in self.openPnl.iterrows():
                try:
                    data = backTestTools.getHistData1Min(timestamp=timestamp,
                                                      symbol=row['Symbol'],
                                                      conn=self.conn)
                    # Update current price for the symbol
                    self.openPnl.at[index, 'CurrentPrice'] = data['c']
                except Exception as e:
                    logging.info(e)
            self.pnlCalculator()
            # Exitcondition
            if not self.openPnl.empty:
                for index,row in self.openPnl.iterrows():
                    if row['PositionStatus']==-1 and row['CurrentPrice']>=row['EntryPrice']*(1+lossValuePercent):
                        self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timestamp)] + [row['Symbol']] +\
                            [row['EntryPrice']] +\
                            [row['CurrentPrice']] +\
                            [row['PositionStatus']*row['Quantity']] +\
                            [0]+["Losshit"]
                        self.openPnl.drop(index, inplace=True)
                    elif row['PositionStatus']==-1 and row['CurrentPrice']<=row['EntryPrice']*rewardPercent:
                        self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timestamp)] + [row['Symbol']] +\
                            [row['EntryPrice']] +\
                            [row['CurrentPrice']] +\
                            [row['PositionStatus']*row['Quantity']] +\
                            [0]+["TargetHit"]
                        self.openPnl.drop(index, inplace=True)
                    time_component = humanTime.time()
                    time_to_compare = datetime.time(15, 15)
                    if time_component>=time_to_compare:
                        self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timestamp)] + [row['Symbol']] +\
                            [row['EntryPrice']] +\
                            [row['CurrentPrice']] +\
                            [row['PositionStatus']*row['Quantity']] +\
                            [0]+["TimeUP"]
                        self.openPnl.drop(index, inplace=True)

                
                self.openPnl.reset_index(drop=True, inplace=True)
          # entrycondition->
            time_component = humanTime.time()
            time_to_compare = datetime.time(9, 30)
            if Trade==True and time_component>=time_to_compare:
                Trade=False
                indexData=backTestTools.getHistData1Min(timestamp=timestamp,
                                                        symbol=indexName,
                                                        conn=self.conn)
                callSym = self.getCall(indexPrice=indexData['c'], strikeDist=strikeDist,
                                       symWithExpiry=symWithExpiry, otmFactor=0)

                putSym = self.getPut(indexPrice=indexData['c'], strikeDist=strikeDist,
                                     symWithExpiry=symWithExpiry, otmFactor=0)

                # Fetch option price 1 min candle data from the database

                callData = backTestTools.getHistData1Min(
                    timestamp=timestamp, symbol=callSym, conn=self.conn)
                putData = backTestTools.getHistData1Min(
                    timestamp=timestamp, symbol=putSym, conn=self.conn)
                logging.info(f'Data for {callSym} is {callData}')
                logging.info(f'Data for {putSym} is {putData}')

                callPrice = self.entryOrder(data=callData, symbol=callSym,
                                        quantity=quantity, entrySide='SELL')
                logging.info(f'Callsell {callPrice}')

                putPrice = self.entryOrder(data=putData, symbol=putSym,
                                        quantity=quantity, entrySide='SELL')
                logging.info(f'PutSell {putPrice}')


            # Close All Position in OpenPnl at the end of day    
            
        self.pnlCalculator()
   




        
        


if __name__ == "__main__":

    import multiprocessing as mp
    from time import sleep

    # Object creation
    obj = algoLogic()

    '''
        Algo to be run between the below days, as this is an intraday algo,
        each day is run as a seperate process using the multiprocessing
        module
    '''

    # Defining all the input parameters

    strikeDist = 50
    baseSym = 'NIFTY'
    indexName = 'NIFTY 50'
    lossValuePercent = 0.3
    rewardPercent = 0.5
    quantity = 50
    userName = 'InternDB'  # Enter provided username
    password = 'mudrakshInternDB'  # Enter provided password

    startDate = datetime.date(2021, 7, 29)
    endDate = datetime.date(2021, 9, 30)


    while endDate >= startDate:

        day = startDate.day
        dayName = startDate.strftime("%A")
        month = startDate.month
        year = startDate.year
        strategyname="Straddle"

        startTime = datetime.datetime(year, month, day, 9, 15, 0)

        # Get the expiry for the given start date
        expiry = backTestTools.getCurrentExpiry(startTime.timestamp())

        endTime = datetime.datetime(year, month, day, 15, 25, 0)

        symWithExpiry = baseSym+expiry

        '''
            Parallel processing is used to speed up the backtesting process as multiple
            days can be tested at once
        '''

        p1 = mp.Process(target=obj.run,
                        kwargs={
                            'startDateTime': startTime,
                            'endDateTime': endTime,
                            'strikeDist': strikeDist,
                            'symWithExpiry': symWithExpiry,
                            'indexName': indexName,
                            'lossValuePercent': lossValuePercent,
                            'rewardPercent': rewardPercent,
                            'quantity': quantity,
                            'userName': userName,
                            'password': password
                        }).start()

        # adjust the gap between two algo runs at your discretion
        sleep(2)

        # Update the date to run the next day
        startDate += datetime.timedelta(days=1)
   
