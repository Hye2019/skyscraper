import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import _6GEO_DATA as gd
import _4DATABASE_INTERFACE as di
import _2PAGE_SCRAPER as ps
import _1URL_SCRAPER as ws
import numpy as np
import time
from multiprocessing.pool import ThreadPool
import datetime
from tqdm import tqdm
import logging
from selenium.webdriver.remote.remote_connection import LOGGER
import os
from pathlib import Path
import json
import sys
from fake_useragent import UserAgent
''' executes the entire scraper '''
SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
BROWSERPATH = ''
SYSTEMOS = sys.platform

LOGGER.setLevel(logging.ERROR)

class Input():
    ''' base class for all user input '''
    def inputNumber(self, prompt):
        while True:
            try:
                userInput = int(input(prompt))       
            except ValueError:
                print("Not an integer! Try again.")
                continue
            else:
                self.inputNum = userInput
                break
    def inputString(self, prompt):
        self.userString = input(prompt)

class InputThread(Input):
    ''' class for getting thread num user input, inherits input class'''
    def __init__(self):
        prompt = "Number of threads to use... "
        print("Sample Data:")
        print("No threading (Powell River) 7:44.56")
        print("2 Threads (Powell River) 4:38.31")
        print("4 Threads (Powell River) 2:49.42")
        print("8 Threads (Powell River) 2:47.69 --- Needs at least 8GB of RAM")
        print("----------------------------------------------------------------------------------")
        print("NOTE: Performance boost from thread count is NOT linearly proportionate.")
        print("AKA: Pick a number 4 to 16, but, more threads needs more RAM.")
        super().inputNumber("Number of threads to use... ")

def ScanEnvironment():
    ''' Attempts to auto locate required dependencies in this code's directory '''
    ''' If fail, falls back to GetEnvironment for manual path input '''
    global SYSTEMOS
    if SYSTEMOS == 'win32':
        print("Scan Environment: OS is Win32")
        chromeDriver = os.path.join(SCRIPTDIR, 'chromedriver.exe')
        chromeDriver = Path(chromeDriver)
        if os.path.isfile(chromeDriver) is False:
            print("Scan Environment: chromedriver.exe not found")
        elif os.path.isfile(chromeDriver) is True:
            print("Scan Environment: Found chromedriver.exe!")
            return chromeDriver
    elif SYSTEMOS == 'darwin':
        print("Scan Environment: OS is darwin/macOS")
        chromeDriver = os.path.join(SCRIPTDIR, 'chromedriver')
        chromeDriver = Path(chromeDriver)
        if os.path.isfile(chromeDriver) is False:
            print("Scan Environment: chromedriver not found")
        elif os.path.isfile(chromeDriver) is True:
            print("Scan Environment: Found chromedriver.exe!")
            return chromeDriver
    return(False)

def GetEnvironment():
    ''' Gets environment variables '''
    global BROWSERPATH
    path = ScanEnvironment()
    if path == False:
        print("Get Environment: Scan Environment could not auto locate dependencies")
        while True:
            driverPath = input("Enter chromedriver path...")
            print(driverPath)
            if os.path.isfile(driverPath):
                print("Get Environment: Found chromedriver.exe!")
                BROWSERPATH = str(driverPath)
                break
            else:
                print("Get Environment: There is no chromedriver.exe at this path")
                pass
    elif path != False:
        BROWSERPATH = str(path)

def getDateNow():
    ''' gets the current date in year-month-day format'''
    datetimenow = datetime.datetime.now()
    currentdate = datetimenow.strftime("%Y-%m-%d")
    return currentdate

def threadingSetup(threadCount):
    ''' sets thread pool size '''
    # Number of workers/threads
    pool = ThreadPool(processes=threadCount)
    print(pool._processes, " threads chosen----------------------------------------------------------")
    return pool

def FakeUserSetup():
    ua = UserAgent()
    a = ua.random
    user_agent = ua.random

    return user_agent

def BrowserSetup(globalBrowserOptions):
    ''' sets browser options '''
    fake_user = FakeUserSetup()
    globalBrowserOptions.headless = True
    globalBrowserOptions.add_argument('--hide-scrollbars')
    globalBrowserOptions.add_argument('--disable-gpu')
    globalBrowserOptions.add_argument("--log-level=3")  # fatal
    globalBrowserOptions.add_argument(f'user-agent={fake_user}')
    print("Random fake ID: " + fake_user)
    return globalBrowserOptions

def getValueRight(df, value):
    ''' gets and returns the right value of a specified cell in a dataframe'''
    try:
        indexArray = getIndexArrayByValue(df, value)
        #print(indexArray)
        indexArray = moveRight(indexArray)
        finalValue = getValueByIndex(df, indexArray)
        return finalValue
    except:
        pass
        #print(value + ' not found --------------------------------------')

def getIndexArrayByValue(df, value):
    ''' get index array by value of a dataframe '''
    for row in range(df.shape[0]):
        for col in range(df.shape[1]):
            if df.at[row,col] == value:
                #print(row, col)
                indexArray = [row, col]
                return indexArray
                break
    
def getValueByIndex(df, indexArray):
    ''' get value by index of a dataframe '''
    y = indexArray[0]
    x = indexArray[1]
    return df.at[y, x]
    
def moveRight(indexArray):
    ''' move right one cell in a dataframe '''
    indexArray[1] = indexArray[1] + 1
    #print(indexArray)
    return indexArray

def initializeDriver(driverOptions, driverPath):
    '''starts a browser using options'''
    if (driverPath != ''):
        browser = webdriver.Chrome(driverPath, options = driverOptions)
    else:
        browser = webdriver.Chrome(options = driverOptions)
    return browser

def Average(lst):
    ''' the average of a inputted list '''
    return sum(lst) / len(lst)

# MANUAL INPUT FUNCTIONS.
# PUTS [foo] INTO IT'S CORRESPONDING CELL LOCATION.
def putMLS(df, mls):
    df['MLS #'] = mls

def putAddress(df, address):
    df['Address'] = address

def putRegionAndActual(df, region):
    df['Region'] = region
    df['Actual Region'] = ExtractRegion(region)
    
def putFinalAskingPrice(df, finalAskingPrice):
    df['Final Asking Price'] = finalAskingPrice
    
def putSellingPrice(df, sellPrice):
    df['Selling Price'] = sellPrice
    
def putSoldDate(df, soldDate):
    df['Sold Date'] = soldDate

def putFeaturesAmenities(df, featuresAmenities):
    df['Features & Amenities'] = str(featuresAmenities)
    
def putRoomInformation(df, roomInfo):
    if roomInfo is not None:
        rIStr = roomInfo.to_string()
        df['Room Information'] = rIStr
    
def putSalesHistory(df, salesHistory):
    if salesHistory is not None:
        sHStr = salesHistory.to_string()
        df['MLS Sales History'] = sHStr 

def getRegionAndURLS(globalBrowserOptions):
    ''' GET URL LIST WITH REGION DATA.'''
    regionandURLS = ws.main(globalBrowserOptions, BROWSERPATH)
    return regionandURLS

def chunkListings(URLS, chunks):
    ''' split url list into chunks for threading '''
    return (np.array_split(URLS, chunks))

def ScrapePage(myThreadNum, region, globalBrowserOptions, dfmain, myURLChunk):
    ''' each thread executes this function on it's own chunk of work '''
    ''' scrapes data for each listing url of it's chunk and adds to a buildingDF. '''
    ''' once all urls of it's chunk have been looped through, the buildingDF is returned '''

    browserForPageScrape = initializeDriver(globalBrowserOptions, BROWSERPATH)
    with tqdm(total = myURLChunk.size, desc=f"Thread {myThreadNum}", position = myThreadNum, unit = "listing", leave = True) as pbar:
        for currentURL in myURLChunk:

            buildingDF = pd.DataFrame(np.nan, index=[0], columns=['MLS #', 'Address', 'Region', 'Actual Region', 'Final Asking Price', 'Selling Price', 'Sold Date', 'Listing Date', 'Property Type', 'Style of House', 'Bedrooms', 'Bathrooms', 'Size of House', 'Price per SqFt', 'Basement', 'Age of House', 'Property Taxes', 'Ownership Interest', 'Days on Market', 'Listing Provided By', "Buyer's Brokerage", 'Features & Amenities', 'Room Information', 'Storeys (Finished)', 'Floor Area (Finished)', 'Strata Fee', 'Rules', 'Roof', 'Flooring', 'Exterior Finish','Foundation', 'Outdoor Features', 'Parking (Total/Covered)', 'Heating', 'Water Supply', 'Zoning', 'MLS Sales History', 'Lat', 'Lon']) 
            buildingDF = buildingDF.astype('object')

            # EXTRACT LISTING INFO FOR CURRENT URL, ADD TO VARIABLE
            listingData = ps.main(browserForPageScrape, currentURL)
            #listingData = ps.main('https://www.zealty.ca/mls.php?id=R2351589&u=heidi.ye@icloud.com&email=')
            # EXTRACT EACH CHART.
                # LS = List, DF = Dataframe
            pdDF = listingData.get('PropertyDetails')
            faLS = listingData.get('FeaturesAndAmenities')
            riDF = listingData.get('RoomInfo')
            mdDF = listingData.get('moreDetailsDF')
            shDF = listingData.get('SalesHistory')
            mls = listingData.get('MLSNumber')
            adr = listingData.get('Address')
            solddate = listingData.get('SoldDate')

            # ASSIGN COMPATABLE DATAFRAMES WITH CHARTS.
            dataLocation =  {
            "Property Type": pdDF,
            "Style of House": pdDF,
            "Bedrooms": pdDF,
            "Bathrooms": pdDF,
            "Selling Price": pdDF,
            "Final Asking Price": pdDF,
            "Asking Price": pdDF,
            "Size of House": pdDF,
            "Price per SqFt": pdDF,
            "Basement": pdDF,
            "Age of House": pdDF,
            "Property Taxes": pdDF,
            "Ownership Interest": pdDF,
            "Listing Date": pdDF,
            "Days on Market": pdDF,
            "Listing Provided By": pdDF,
            "Buyer's Brokerage": pdDF,
            "Storeys (Finished)": mdDF,
            "Floor Area (Finished)": mdDF,
            "Strata Fee": mdDF,
            "Rules": mdDF,
            "Roof": mdDF,
            "Flooring": mdDF,
            "Exterior Finish": mdDF,
            "Foundation": mdDF,
            "Outdoor Features": mdDF,
            "Parking (Total/Covered)": mdDF,
            "Heating": mdDF,
            "Water Supply": mdDF,
            "Zoning": mdDF,
            "Room Information": riDF
            }

                # EXTRACT DATA FROM TABLES AND ADD TO CURRENT BUILDING ROW

            # MANUAL INPUT (Special Data Types)

            putMLS(buildingDF, mls)
            putAddress(buildingDF, adr)
            putRegionAndActual(buildingDF, region)

            putSoldDate(buildingDF, solddate)
            putFeaturesAmenities(buildingDF, faLS)

            #three column tables special
            putRoomInformation(buildingDF, riDF)
            putSalesHistory(buildingDF, shDF)
            

            ''' get either final asking price or asking price and put into asking price's cell depending on which price is present in the scraped data '''
            if pdDF[0].str.contains('Final Asking Price').any():
                putFinalAskingPrice(buildingDF, getValueRight(pdDF, 'Final Asking Price'))
            elif pdDF[0].str.contains('Asking Price').any():
                putFinalAskingPrice(buildingDF, getValueRight(pdDF, 'Asking Price'))

            # AUTO INPUT (Two Column Tables)
            # Don't take sold date, features & amenities, room info, MLS Sales hist. or final asking price since those are being manually inputted.
            headers = list(dfmain.columns)
            #print(headers)
            for header in headers:
                currentIndex = headers.index(header)
                #if(buildingDF.at[0, currentIndex]==np.nan):
                if (np.all((pd.isnull(buildingDF.iloc[0, currentIndex]))) and (header != 'Sold Date') and (header != 'Features & Amenities') and (header != 'Room Information') and (header != 'MLS Sales History') and (header != 'Final Asking Price') and (header != 'Lat') and (header != 'Lon') and (header != 'Actual Region')):
                    buildingDF.iloc[0, currentIndex] = getValueRight(dataLocation[header], header)

            #print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            #print(buildingDF.values.tolist())
            #print("----------------------------------------------------------------------------------------------------------")

            ''' add current scraped listing data to the main dataframe as a row'''
            dfmain = dfmain.append(buildingDF)
            # Reset current building dataframe.
            buildingDF = pd.DataFrame(np.nan, index=[0], columns=['MLS #', 'Address', 'Region', 'Actual Region', 'Final Asking Price', 'Selling Price', 'Sold Date', 'Listing Date','Property Type', 'Style of House', 'Bedrooms', 'Bathrooms', 'Size of House', 'Price per SqFt', 'Basement', 'Age of House', 'Property Taxes', 'Ownership Interest', 'Days on Market', 'Listing Provided By', "Buyer's Brokerage", 'Features & Amenities', 'Room Information', 'Storeys (Finished)', 'Floor Area (Finished)', 'Strata Fee', 'Rules', 'Roof', 'Flooring', 'Exterior Finish','Foundation', 'Outdoor Features', 'Parking (Total/Covered)', 'Heating', 'Water Supply', 'Zoning', 'MLS Sales History', 'Lat', 'Lon']) 
            buildingDF = buildingDF.astype('object')
    
            pbar.update()

    # Wait for remaining network calls to finish and then close browser.
    time.sleep(5)
    browserForPageScrape.quit()
    return dfmain

def GiveThreadsWork(region, globalBrowserOptions, pagePool, chunkedListings, asyncResults, dfmain):
    ''' starts each thread and gives each thread it's work '''
    count = 0
    for theirWork in chunkedListings:
        async_result = pagePool.apply_async(ScrapePage, (count, region, globalBrowserOptions, dfmain, theirWork))
        asyncResults.append(async_result)
        count += 1
    return asyncResults

def GetThreadsWork(dfmain, asyncResults):
    ''' gets the result of each thread and recombines all work into one dataframe '''
    for async_result in asyncResults:
        dfmain = dfmain.append(async_result.get())
    return dfmain

def ThreadWork(region, globalBrowserOptions, pagePool, chunkedListings, asyncResults, dfmain):
    ''' combines givethreadwork and getthreadswork '''
    asyncResults = GiveThreadsWork(region, globalBrowserOptions, pagePool, chunkedListings, asyncResults, dfmain)
    dfmain = GetThreadsWork(dfmain, asyncResults)
    pagePool.close()
    return dfmain

def OutputToCSV(filename, dfmain):
    ''' outputs given dataframe as a csv given a filename. '''
    out_file = open(f"{filename}.csv",'w', newline='')
    out_file.write(dfmain.to_csv())
    out_file.close()
    print ("Success")
    print ("Saved as: " + filename + " at " + os.getcwd())

def DFCleanup(df):
    df = df.reset_index()
    df = df.drop(columns="index")
    return df

def ConnectSSH():
    tunnel = di.SSHTunnel()
    tunnel.start()
    print("Connection Successful!")
    print(f"Connected Via: {tunnel.local_bind_host}, {tunnel.local_bind_port}")
    #tunnel.stop()
    return tunnel

def DisconnectSSH(tunnel):
    print("Cleaning up SSH tunnel...")
    tunnel.stop()

def NetworkCleanup(tunnel, dbEngine):
    di.DisconnectEngine(dbEngine)
    DisconnectSSH(tunnel)

def Output(filename, dfmain, region, dbEngine, schema, localBindPort):
    ''' Outputs master dataframe to all supported formats. '''
    print("Outputting to CSV...")
    OutputToCSV(filename, dfmain)

    print("Outputting to JSON...")
    dfmain.to_json(rf'{os.getcwd()}\{filename}.json', orient='records')

    print("Outputting as SQL to postgresql database...")
    di.OutputDFToSQL(dfmain, region, dbEngine, schema, localBindPort)

def ExtractRegion(region):
    ''' looks for sold keyword in region and removes if necessary '''
    if 'SOLD' in region:
        clippedRegion = region[:-5]
        return clippedRegion
    else:
        return region

def main():
    GetEnvironment()
    tunnel = ConnectSSH()

    dbCredentials = di.GetDBCredentials()
    dbEngine = di.connectDatabase(dbCredentials, tunnel.local_bind_port)

    ''' runs the scraper '''
    dfmain = pd.DataFrame(columns=['MLS #', 'Address', 'Region', 'Actual Region', 'Final Asking Price', 'Selling Price', 'Sold Date', 'Listing Date', 'Property Type', 'Style of House', 'Bedrooms', 'Bathrooms', 'Size of House', 'Price per SqFt', 'Basement', 'Age of House', 'Property Taxes', 'Ownership Interest', 'Days on Market', 'Listing Provided By', "Buyer's Brokerage", 'Features & Amenities', 'Room Information', 'Storeys (Finished)', 'Floor Area (Finished)', 'Strata Fee', 'Rules', 'Roof', 'Flooring', 'Exterior Finish','Foundation', 'Outdoor Features', 'Parking (Total/Covered)', 'Heating', 'Water Supply', 'Zoning', 'MLS Sales History', 'Lat', 'Lon']) 
    asyncResults = []

    threadCount = InputThread().inputNum
    pagePool = threadingSetup(threadCount)
    globalBrowserOptions = Options()
    globalBrowserOptions = BrowserSetup(globalBrowserOptions)
    regionandURLS = getRegionAndURLS(globalBrowserOptions)

    # EXTRACT GEO AND URL DATA
    region = regionandURLS[0]
    actualRegion = ExtractRegion(region)
    listingURLS = regionandURLS[1]

    filename = f"{region}{getDateNow()}"
    # Split URL list into chunks for each worker to take.
    chunkedListings = chunkListings(listingURLS, pagePool._processes)

    dfmain = ThreadWork(region, globalBrowserOptions, pagePool, chunkedListings, asyncResults, dfmain)
    dfmain = DFCleanup(dfmain)

    # add coordinate data.
    dfmain = gd.main(dfmain, actualRegion, region, dbEngine, tunnel.local_bind_port)

    print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX--- FINAL MAIN DATAFRAME ---XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    print(dfmain)

    Output(filename, dfmain, region, dbEngine, 'Listings', tunnel.local_bind_port)
    NetworkCleanup(tunnel, dbEngine)


    print("Done!")
    exit(0)
    
main()