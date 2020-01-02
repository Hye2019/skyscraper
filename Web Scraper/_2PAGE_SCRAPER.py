from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
from pandas.io.html import read_html
import os
import time
import sys
''' Scrapes data from a single page '''


def getPropertyDetails(browser):
    '''scrapes property details chart.'''
    ''' also catches antiscraper problems, and waits two minutes for three times '''
    #browser = launchURL(url)
    tableTitle = browser.find_element_by_xpath("//*[contains(text(), 'Property Details')]")
    table = tableTitle.find_element_by_xpath("following-sibling::*[1]")
    table_html = table.get_attribute('outerHTML')
    df = read_html(table_html)[0]
    return df

def getFeatures(browser):
    '''scrapes features and amentities chart'''
    #browser = launchURL(url)
    try:
        fAATitle = browser.find_element_by_xpath("//*[contains(text(), 'Features & Amenities')]")
        fAAParent = fAATitle.find_element_by_xpath("following-sibling::*[1]")
        fAAList = fAAParent.find_element_by_class_name('striped')
        fAAChildren = fAAList.find_elements_by_tag_name('li')
        #print("Features & Amenities")
        featuresList = []
        for i in range(len(fAAChildren)):
            featuresList.append(fAAChildren[i].text)
        return featuresList
    except:
        pass
        #print("No features and amenities table found. ------------------------------------------")

#print(getFeatures('https://www.zealty.ca/mls.php?id=R2351589&u=heidi.ye@icloud.com&email='))

def getRoomInfo(browser):
    '''scrapes room info chart if it exists'''
    #browser = launchURL(url)
    try:
        rITitle = browser.find_element_by_xpath("//*[contains(text(), 'Room Information')]")
        rI = rITitle.find_element_by_xpath("following-sibling::*[1]")
        roomInfoTable = rI.get_attribute('outerHTML')
        rIT = read_html(roomInfoTable)[0]
        return rIT
    except:
        pass
        #print("No room information table found. ------------------------------------------")

#print(getRoomInfo('https://www.zealty.ca/mls.php?id=R2351589&u=heidi.ye@icloud.com&email='))
    
def getMoreDetails(browser):
    '''scrapes more details chart if it exists'''
    #browser = launchURL(url)
    try:
        mDTitle = browser.find_element_by_xpath("//*[contains(text(), 'More Details')]")
        mD = mDTitle.find_element_by_xpath("following-sibling::*[1]")
        moreDetailTable = mD.get_attribute('outerHTML')
        mDT = read_html(moreDetailTable)[0]
        mDT.drop([2], axis=1, inplace=True)
        return mDT
    except:
        pass
        #print("No more detail table found. ------------------------------------------")
        
#print(getMoreDetails('https://www.zealty.ca/mls.php?id=R2351589&u=heidi.ye@icloud.com&email='))
        
def getSalesHistory(browser):
    '''scrapes sales history chart'''
    #browser = launchURL(url)
    try:
        table = browser.find_element_by_xpath('//*[@id="soldHistory"]/div/table')
        table_html = table.get_attribute('outerHTML')
        df = read_html(table_html)[0]
        return df
    except:
        pass
        #print("No sales history table found. ------------------------------------------")

#print(getSalesHistory('https://www.zealty.ca/mls.php?id=R2351589&u=heidi.ye@icloud.com&email='))

def getAddress(browser):
    '''scrapes address'''
    #browser = launchURL(url)
    address = browser.find_element_by_xpath('//*[@id="results"]/table/tbody/tr/td[1]/span[1]')
    return address.text

def getSoldDate(browser):
    '''scrapes sold date IF available, tests by taking first four chars which should be "Sold" '''
    try:
        soldDate = browser.find_element_by_xpath('//*[@id="results"]/table/tbody/tr/td[2]/span[1]')
        verifying = soldDate.text
        if "SOLD" in verifying:
            return verifying
        else:
            pass
    except:
        pass
        #print("No sold date found. ------------------------------------------")

def getMLSNumber(url):
    '''returns MLS number from URL'''
    identifier = url[33:41]
    return identifier
    
    
def main(browser, urlToAnalyze):
    '''scrapes given URL and compiles data into dictionary to be returned to master'''
    tryCount = 3
    triedCount = 0
    while True:
        if tryCount == 0:
            sys.exit('(ANTI-ANTI SCRAPER) TRIED ', triedCount, ' TIMES, EXITING.')
            exit(1)
        try:
            # initializing web browser with given url.
            browser.get(urlToAnalyze)

            propertyDetailsDF = getPropertyDetails(browser)
            featuresAndAmenitiesLS = getFeatures(browser)
            roomInfoDF = getRoomInfo(browser)
            moreDetailsDF = getMoreDetails(browser)
            salesHistoryDF = getSalesHistory(browser)
            address = getAddress(browser)
            mlsNum = getMLSNumber(urlToAnalyze)
            soldDate = getSoldDate(browser)
            
            listingData = {
                'PropertyDetails': propertyDetailsDF,
                'FeaturesAndAmenities': featuresAndAmenitiesLS,
                'RoomInfo': roomInfoDF,
                'MoreDetails': moreDetailsDF,
                'SalesHistory': salesHistoryDF,
                'Address': address,
                'MLSNumber': mlsNum,
                'SoldDate': soldDate
            }
            break
        except:
            print("(ANTI-ANTI SCRAPER): ACTIVATED. WAITING TWO MINUTES. +X+X+X+X+XX+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X+X")
            print("(ANTI-ANTI SCRAPER): TRIED ", triedCount, " TIME(S).")
            print("(ANTI-ANTI SCRAPER): ", tryCount, " TIMES LEFT.")
            time.sleep(120)
            tryCount -= 1
            triedCount += 1
            continue
 
    return listingData
        
        