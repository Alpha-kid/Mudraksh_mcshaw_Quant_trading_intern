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
            indexName, lossValuePercent, rewardMultiple,
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

        '''
            Creating file name for the particular run, format is similar to logFile
            
            IMPORTANT - 
            
            Don't change the file name formats as this will generate incompatible output files which
            won't run in our analytics softwares.

        '''
        # dataframe_day=getBackTestData1Min(startDateTime,endDateTime,symWithExpiry)
        # for i in range(0,len(dataframe_day)):
        #     exitcond(i,dataframe_day)
        #     entrycond(i,dataframe_day)


        
        pnlDate = str(startDateTime)
        pnlDate += indexName
        self.writeFileLocation += pnlDate

        '''--------------------------------------------------------------------------'''

        unrealizedPnl = 0
        realizedPnl = 0
        netPnl = 0

        # Fetch index price 1 min candle data from the database

        indexData = backTestTools.getHistData1Min(
            timestamp=startTimeEpoch, symbol=indexName, conn=self.conn)

        ''' Its very important to log the data recieved from the data,
            in order to verify any incorrect data recieved
        '''

        logging.info(f'Index Price is {indexData["c"]}')
        # Set the global timeVariable
        self.timeData = startTimeEpoch

        # Get the ATM call and put Symbols for the revlevant methods

        callSym = self.getCall(indexPrice=indexData['c'], strikeDist=strikeDist,
                               symWithExpiry=symWithExpiry, otmFactor=0)

        putSym = self.getPut(indexPrice=indexData['c'], strikeDist=strikeDist,
                             symWithExpiry=symWithExpiry, otmFactor=0)

        # Fetch option price 1 min candle data from the database
        
        callData = backTestTools.getHistData1Min(
            timestamp=startTimeEpoch, symbol=callSym, conn=self.conn)
        putData = backTestTools.getHistData1Min(
            timestamp=startTimeEpoch, symbol=putSym, conn=self.conn)
        logging.info(f'Data for {callSym} is {callData}')
        logging.info(f'Data for {putSym} is {putData}')

        callPrice = self.entryOrder(data=callData, symbol=callSym,
                                    quantity=quantity, entrySide='BUY')

        putPrice = self.entryOrder(data=putData, symbol=putSym,
                                   quantity=quantity, entrySide='BUY')

        # Set the loss limit to a particular percent of the total value of option solds

        lossLimit = (-1)*(callPrice+putPrice)*quantity*lossValuePercent

        logging.info(f'Loss limit {lossLimit}')

        # set target to a multiple of loss limit

        targetPnl = abs(lossLimit)*rewardMultiple

        logging.info(f'Target Pnl is {targetPnl}')

        exitReason = 'TimeUp'
        Data_frame=backTestTools.getBackTestData1Min(startDateTime,endDateTime,symWithExpiry)
        for i in range(0,len(Data_frame)):
            humanTime = Data_frame.index(0)

            if not self.closedPnl.empty:
                realizedPnl = self.closedPnl['Pnl'].sum()
                netPnl = unrealizedPnl+realizedPnl
            logging.info(f'------------Time {humanTime}---------------')
            for index, row in self.openPnl.iterrows():
                    
                    try:

                        # Get data for all the symbols present in the openPnl df one by one for further processing
                        data = backTestTools.getHistData1Min(timestamp=timeData,
                                                             symbol=row['Symbol'],
                                                             conn=self.conn)

                        # Update current price for the symbol
                        self.openPnl.at[index, 'CurrentPrice'] = data['c']

                    except Exception as e:
                        logging.info(e)
            logging.info(f'Current Unrealized Pnl: {unrealizedPnl}')
            logging.info(f'Current Realized Pnl: {realizedPnl}')
            logging.info(f'Current Net Pnl: {netPnl}')
            if not self.openPnl.empty():
                callData = backTestTools.getHistData1Min(timestamp=Data_frame.index(i), symbol=callSym, conn=self.conn)
                putData = backTestTools.getHistData1Min(timestamp=Data_frame.index(i), symbol=putSym, conn=self.conn)
                if(callData['c']+putData['c']<lossLimit):
                    logging.info(f'Loss hit hit')
                    exitReason = 'Loss limit hit'
                    break

                elif(callData['c']+putData['c']>targetPnl):
                    logging.info(f'Target hit hit')
                    exitReason = 'Target hit'
                    break
                
            

        for index, row in self.openPnl.iterrows():
            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(timeData)] + [row['Symbol']] +\
                [row['EntryPrice']] +\
                [row['CurrentPrice']] +\
                [row['PositionStatus']*row['Quantity']] +\
                [0]+[exitReason]
        self.pnlCalculator()
        logging.info(f'Net pnl for the day is {self.closedPnl["Pnl"].sum()}')


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
    rewardMultiple = 2
    quantity = 50
    userName = 'InternDB'  # Enter provided username
    password = 'mudrakshInternDB'  # Enter provided password

    startDate = datetime.date(2021, 9, 29)
    endDate = datetime.date(2021, 9, 30)

    while endDate >= startDate:

        day = startDate.day
        dayName = startDate.strftime("%A")
        month = startDate.month
        year = startDate.year
        strategyname="Straddle"

        startTime = datetime.datetime(year, month, day, 9, 30, 0)

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
                            'rewardMultiple': rewardMultiple,
                            'quantity': quantity,
                            'userName': userName,
                            'password': password
                        }).start()

        # adjust the gap between two algo runs at your discretion
        sleep(2)

        # Update the date to run the next day
        startDate += datetime.timedelta(days=1)
    closedPnl.to_csv(f'{baseSym}+{startDate}+{endDate}+{strategyname}.csv')
