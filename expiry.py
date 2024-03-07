import pandas as pd
from datetime import datetime
from histData import connectToMongo


def getExpiryData(date, sym):
    try:
        if isinstance(date, datetime):
            getDatetime = date
        elif isinstance(date, float):
            getDatetime = datetime.fromtimestamp(date)
        else:
            raise Exception(
                "date is not a timestamp(float) or datetime object")

        expiryDict = None

        conn = connectToMongo()

        db = conn["testing"]
        collection = db[f"FNO_Expiry_BT"]

        rec = collection.find({'Sym': sym})
        rec = list(rec)

        if rec:
            df = pd.DataFrame(rec)
            df["Date"] = pd.to_datetime(df["Date"])
            df["Date"] = df["Date"] + pd.Timedelta(hours=15, minutes=30)
            df.set_index("Date", inplace=True)
            df.sort_index(inplace=True, ascending=True)

            for index, row in df.iterrows():
                if getDatetime <= index:
                    expiryDict = row.to_dict()
                    break

        '''Checking another collection if expiry not found'''
        if not expiryDict:
            collection = db[f"FNO_Expiry"]

            rec = collection.find({'Sym': sym})
            rec = list(rec)

            if rec:
                df = pd.DataFrame(rec)
                df["Date"] = pd.to_datetime(df["Date"])
                df["Date"] = df["Date"] + pd.Timedelta(hours=15, minutes=30)
                df.set_index("Date", inplace=True)
                df.sort_index(inplace=True, ascending=True)

                for index, row in df.iterrows():
                    if getDatetime <= index:
                        expiryDict = row.to_dict()
                        break
        return expiryDict

    except Exception as e:
        raise Exception(e)


'''Create a dataframe with all expiry then check which expiry is just greater than given date'''
