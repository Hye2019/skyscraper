import _4DATABASE_INTERFACE as di
import numpy as np
import datetime
import pandas as pd
TODAY = datetime.date.today()

''' Manages tomtom API credits '''

def getTable(engine):
    ''' Grabs API Keys table from server '''
    print("running get api table function")
    return di.SQLTableToDF('API Keys', engine, 'Tomtom')

def getColumn(df, name):
    ''' General function for grabbing one column of a table '''
    print("running get column function")
    ''' get value of column '''
    ''' returns column from dataframe as list '''
    dfColumn = df[name].tolist()
    return dfColumn

def IsEnoughCredits(viableAPIDf, lenUrls):
    ''' Checks if there are enough tomtom credits to process the amount of URLS specified '''
    clList = getColumn(viableAPIDf, 'Credits_Left')
    totalAvailableCredits = 0
    for x in clList:
        totalAvailableCredits += x
    print(f'Total Available Credits: {totalAvailableCredits}')
    print(f'Needed Credits: {lenUrls}')
    if lenUrls > totalAvailableCredits: return False
    elif lenUrls <= totalAvailableCredits: return True
    else:
        print("Failed to meet any conditions in IsEnoughCredits")
        exit(1)

def resetkey(APIkeydf):
    ''' In tomtom api key table, updates credits left for each key based on time of day '''
    print("running resetkey function")
    for row in APIkeydf.itertuples():
        clCell = int(getattr(row, "Credits_Left")) # cl is credits left
        lmCell = getattr(row, "Last_Modified") # lm is last modified
        print(f'Credits left and last modified in current processing row: {clCell}, {lmCell}')
        if lmCell != str(TODAY):
            print(f"{lmCell} is not {TODAY}, updating necessary values.")
            print(f"Updating {clCell} to 2500")
            print(f"the current row of dataframe we are processing:   {row}")
            APIkeydf.at[getattr(row, "Index"), 'Credits_Left'] = 2500
            print("After line 252")
        else:
            print("this api key's date is already today")
    return APIkeydf

def isViableKey(updatedAPIDf):
    ''' Compiles a table of only API keys with credits left for use in processing '''
    print("running is viable key function")
    viableAPIDf = pd.DataFrame(columns = [
        'Key',
        'Credits_Left', 'Last_Modified']
        )
    for row in updatedAPIDf.itertuples():
        clCell = int(getattr(row, "Credits_Left")) # cl is credits left
        inCell = getattr(row, "Index") # in is index
        if clCell != 0:
            print(f'{clCell} credits at index {inCell} is viable, adding to viableapidf')
            rowToAppend = updatedAPIDf.iloc[inCell]
            viableAPIDf = viableAPIDf.append(rowToAppend)
            print("VIABLE API DF SO FAR")
            print(viableAPIDf)
    return viableAPIDf

def updateDate(APIkeydf, region, localPort, engine):
    ''' Sets last used date in API key table to today '''
    print("running update date function")
    print(TODAY)
    print(type(TODAY))
    print('BEFORE SETTING DATE TO TODAY')
    print(APIkeydf)
    APIkeydf['Last_Modified'] = str(TODAY)
    print('AFTER SETTING DATE TO TODAY')
    print(APIkeydf)
    return APIkeydf

def main(df, actualRegion, region, dbEngine, localPort):
    ''' Grabs API Key table from server, updates dates and returns a table of viable API keys to master. '''
    print("getting api viable key table..")
    APIkeydf = getTable(dbEngine)
    print(APIkeydf)
    BASEURL = 'https://api.tomtom.com'
    HEADERS = {'Content-type': 'application/json'}
    geoDF = pd.DataFrame(columns = [
        'Query', 
        'Country', 
        'Country Subdivision', 
        'Country Secondary Subdivision', 
        'Municipality', 
        'Municipality Subdivision', 
        'Freeform Address',
        'Street Name', 
        'Street Number',
        'Lat', 'Lon']
        )
    
    updatedAPIDf = resetkey(APIkeydf)

    viableAPIDf = isViableKey(updatedAPIDf)
    viableAPIDf = updateDate(viableAPIDf, region, localPort, dbEngine)

    return viableAPIDf