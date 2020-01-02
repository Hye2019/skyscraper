import requests
import pandas as pd
import numpy as np
import json
import urllib.parse
import time
import datetime
import _4DATABASE_INTERFACE as di
import _5LOG_API as la

def updateCredit(APIkeydf, engine, localPort):
    print("running update credit function")
    di.OutputDFToSQL(APIkeydf, 'API Keys', engine, 'Tomtom', localPort)

def getIndexArrayByValue(df, value):
    print("running get index array by value function")
    ''' get index array by value of a dataframe '''
    for row in range(df.shape[0]):
        for col in range(df.shape[1]):
            if df.at[row,col] == value:
                #print(row, col)
                indexArray = [row, col]
                return indexArray
                break

def chunkList(viableAPIDf, urls, engine, localPort):
    print("running chunk list function")
    URLSToSubmit = []
    totalURLCount = len(urls)
    if la.IsEnoughCredits(viableAPIDf, totalURLCount) == False:
        print("Not enough API credits to process ")
        input("Failing...")
        exit(1)
    print("Inside while totalurlcount != 0 statement")
    print("viable api dataframe")
    print(viableAPIDf)
    startingIndex = 0 # starting index to remember which urls we've already covered.
    for row in viableAPIDf.itertuples(): # loop through api key row
        currentCL = int(getattr(row, 'Credits_Left')) # currentCL means current cell's credits left
        currentIN = int(getattr(row, 'Index')) # currentIN means current cell's index in y axis
        print(f"CREDITS LEFT OF THIS API KEY {currentCL}")
        print(f"INDEX OF THIS KEY {currentIN}")
        newCL = currentCL #new credits left to be updated to dataframe
        myBatchURLs = []
        for i in range(startingIndex, startingIndex + currentCL):
            if totalURLCount == 0:
                break
            print(f"current index of inner url loop {i}")
            print(f"ACTUAL TOTAL URL COUNT: {len(urls)}")
            print(f"Total URL COUNT as decreasing: {totalURLCount}")
            print(f"STARTING INDEX: {startingIndex}")
            print(f"NEW CREDITS LEFT: {newCL}")
            startingIndex += 1
            totalURLCount -= 1
            newCL -= 1
            myBatchURLs.append(urls[i])
        URLSToSubmit.append(myBatchURLs)
        #startingIndex += 1

        viableAPIDf.at[currentIN, 'Credits_Left'] = newCL
        if totalURLCount == 0:
            break
    updateCredit(viableAPIDf, engine, localPort)

    return URLSToSubmit
            

def constructBatch(addressList, region, APIDICT, engine, localPort):
    ''' constructs valid json query to tomtom based on imputted addresses and region '''
    print("running construct batch function")
    templates = []
    finalJsonQuerys = []

    for address in addressList:
        address = f"{address} {region}"
        encodedAddress = urllib.parse.quote(address)
        template = f"/geocode/{encodedAddress}.json?typeahead=true&limit=1&countrySet=CA&lat=53.726669&lon=-127.647621&topLeft=60.819142%2C-143.460805&btmRight=47.663884%2C-113.311092" # bias towards centre of BC and bound on BC
        templates.append(template)
    chunkedTemplates = chunkList(APIDICT, templates, engine, localPort)
    for chunk in chunkedTemplates:
        myJsonQuerys = []
        jsonBatch = '{"batchItems":[{"query":"' #Start off the batch with the usual json starting format.
        QUERYCONTINUE = '"},{"query":"'
        QUERYEND = '"}]}'
        for listing in chunk:
            if chunk.index(listing) == len(chunk) - 1:
                jsonBatch = jsonBatch + listing + QUERYEND # adding on to the preformatted json.
            else:
                jsonBatch = jsonBatch + listing + QUERYCONTINUE
            #myJsonQuerys.append(jsonBatch)
        finalJsonQuerys.append(jsonBatch)
    #print(finalJsonQuerys)
    #time.sleep(10)
    return finalJsonQuerys

def SubmitBatch(baseURL, apiKeys, headers, querys):
    print("running submit batch function")
    ''' submit json batch request to tomtom api '''
    counter = 0
    resultURLS = []
    for query in querys:
        #print(query)
        r = requests.post(f'https://api.tomtom.com/search/2/batch.json?key={apiKeys[counter]}', headers=headers, data=query, allow_redirects=False)
        print(r.status_code, r.reason, r.headers.get('Location'))
        url = baseURL + r.headers.get('Location')
        resultURLS.append(url)
        counter += 1
    return resultURLS

def GetBatchResult(URLS):
    print("running get batch result function")
    ''' get result of batches '''
    batchResults = []
    for url in URLS:
        while True:
            try:
                print("GETTING THIS:")
                print(url)
                r = requests.get(url)
                data = r.json()
                batchResults.append(data)
                break
            except:
                print('trying again')
                continue

    return batchResults

def ExtractData(dictResults):
    print("running extract data function")
    ''' extract relevent data from resulted batches '''
    #print(dictResults)
    allData = []
    for chunk in dictResults:
        numItems = len(chunk['batchItems'])
        for x in range(0, numItems):
            try:
                query = chunk['batchItems'][x]['response']['summary']['query']
            except:
                query = None 
            try:
                country = chunk['batchItems'][x]['response']['results'][0]['address']['country']
            except:
                country = None
            try:
                countrySubdivision = chunk['batchItems'][x]['response']['results'][0]['address']['countrySubdivision']
            except:
                countrySubdivision = None
            try:
                countrySecondarySubdivision = chunk['batchItems'][x]['response']['results'][0]['address']['countrySecondarySubdivision']
            except:
                countrySecondarySubdivision = None
            try:
                municipality = chunk['batchItems'][x]['response']['results'][0]['address']['municipality']
            except:
                municipality = None
            try:
                municipalitySubdivision = chunk['batchItems'][x]['response']['results'][0]['address']['municipalitySubdivision']
            except:
                municipalitySubdivision = None
            try:
                freeformAddress = chunk['batchItems'][x]['response']['results'][0]['address']['freeformAddress']
            except:
                freeformAddress = None
            try:
                streetName = chunk['batchItems'][x]['response']['results'][0]['address']['streetName']
            except:
                streetName = None
            try:
                streetNumber = chunk['batchItems'][x]['response']['results'][0]['address']['streetNumber']
            except:
                streetNumber = None
            try:
                lat = chunk['batchItems'][x]['response']['results'][0]['position']['lat']
            except:
                lat = None
            try:
                lon = chunk['batchItems'][x]['response']['results'][0]['position']['lon']
            except:
                lon = None
            data = {
                "query": query,
                "country": country,
                "countrySubdivision": countrySubdivision,
                "countrySecondarySubdivision": countrySecondarySubdivision,
                "municipality": municipality,
                "municipalitySubdivision": municipalitySubdivision,
                "freeformAddress": freeformAddress,
                "streetName": streetName,
                "streetNumber": streetNumber,
                "lat": lat,
                "lon": lon
                }
            allData.append(data)
    return allData

def ListToDataframe(dataList):
    print("running list to dataframe function")
    ''' convert list to dataframe '''
    workingDF = pd.DataFrame(columns = [
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
    finalDF = pd.DataFrame(columns = [
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
    dfList = ['Query',
        'Country',
        'Country Subdivision',
        'Country Secondary Subdivision',
        'Municipality',
        'Municipality Subdivision',
        'Freeform Address',
        'Street Name',
        'Street Number',
        'Lat', 'Lon']

    for listingGeo in dataList:
        counter = 0
        for key in listingGeo:
            field = listingGeo.get(key)
            workingDF.at[0, dfList[counter]] = field
            counter += 1
        finalDF = finalDF.append(workingDF)
    return finalDF

def main(df, actualRegion, region, dbEngine, localPort):
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

    viableAPIDf = la.main(df, actualRegion, region, dbEngine, localPort)

    dfAddr = la.getColumn(df, "Address")
    #actualRegion = la.getColumn(df, "Region")
    querys = constructBatch(dfAddr, actualRegion, viableAPIDf, dbEngine, localPort)

    submission = SubmitBatch(BASEURL, la.getColumn(viableAPIDf, "Key"), HEADERS, querys)
    dictResult = GetBatchResult(submission)

    data = ExtractData(dictResult)
    #print(data)

    finalGeoDF = ListToDataframe(data)
    finalGeoDF.insert(0,"MLS #", df["MLS #"].values.tolist(), True)
    finalGeoDF = finalGeoDF.reset_index()

    di.OutputDFToSQL(finalGeoDF, region, dbEngine, 'Geography', localPort)

    coordinateDF = finalGeoDF[['Lat', 'Lon']]

    df.update(coordinateDF)

    return df

#print(jsonQuery)