from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import datetime
import time
''' Scrapes urls from the zealty search page '''


NEXTBUTTON = "//*[@id='footer']/div/button[2]"

def SetupBrowser(URL, browserOptions, browserPath):
    ''' starts a browser and goes to a inputted URL and switches to searchFrame iframe. '''
    if browserPath != '':
        driver = webdriver.Chrome(browserPath, options = browserOptions)
    else:
        driver = webdriver.Chrome(options = browserOptions)
    driver.implicitly_wait(6)
    driver.get(URL)
    driver.switch_to.frame("searchFrame")
    return driver

def Login(driver):
    ''' prompts user for email and password to login to zealty '''
    while True:
        print("dummy e-mail: yipuzac@mytmail.in")
        print("dummy password: asdf")
        print("")
        print("-*-*-*- LOGIN CREDENTIALS -*-*-*-")
        print("Enter email address of account:")
        email = input()
        print("Enter password of account:")
        password = input()
        
        driver.find_element_by_xpath('//*[@id="logBox"]').click()
        
        inputEmail = driver.find_element_by_xpath('//*[@id="loginForm"]/input[1]')
        inputEmail.send_keys(email)
        
        inputPassword = driver.find_element_by_xpath('//*[@id="loginForm"]/input[2]')
        inputPassword.send_keys(password)
        
        loginButton = driver.find_element_by_xpath('//*[@id="loginForm"]/button[2]')
        loginButton.click()
        
        loginStatus = driver.find_element_by_xpath('//*[@id="logGreeting"]')
        try:
            greetingisPresent = loginStatus.find_element_by_tag_name('b')
            print("Logged in!")
            return [email, password]
            break
        except:
            print("Error, try again.")
            continue

def Logout(isLoggedIn, driver):
    ''' logs user out '''
    if isLoggedIn == True:
        logoutButton = driver.find_element_by_xpath('//*[@id="logButton"]')
        logoutButton.click()
    elif isLoggedIn == False:
        print("User not logged in! Can't logout?")

def SelectRegion(driver):
    ''' select region based on user input '''
    while True:
        #Select Geographic Area Dropdown
        geographicDropdown = Select(driver.find_element_by_xpath('//*[@id="searchForm"]/table[2]/tbody/tr/td/select[1]'))
        
        # Select geo area by user input.
        try:
            geoAnswer = input("Enter exact geographical region as written on site ")
            geographicDropdown.select_by_visible_text(geoAnswer)
            return geoAnswer
            break
        except:
            print("Input did not match one of the geographical options, try again.")

def SetSoldRange(driver):
    ''' sets the sold search range to the max time IF we're querying for sold listings '''
    try:
        print("Waiting for database load...")
        time.sleep(2)
        soldRangeDropdown = Select(driver.find_element_by_xpath('//*[@id="soldPopup"]/select'))
        soldRangeDropdown.select_by_visible_text("Anytime")

        submitFilterButton = driver.find_element_by_xpath('/html/body/div[2]/div[1]/table/tbody/tr/td/form/table[3]/tbody/tr[2]/td/div[4]/div[1]/button[2]')
        submitFilterButton.click()
        print("SET TO ANYTIME DAYS ++++++++++++++++++++++++++++++++")

        time.sleep(2)
    except:
        print(f"Anytime not found, trying 24 months")
        try:
            time.sleep(2)
            print("Waiting for database load...")
            soldRangeDropdown = Select(driver.find_element_by_xpath('//*[@id="soldPopup"]/select'))
            soldRangeDropdown.select_by_visible_text("Last 24 months")

            submitFilterButton = driver.find_element_by_xpath('/html/body/div[2]/div[1]/table/tbody/tr/td/form/table[3]/tbody/tr[2]/td/div[4]/div[1]/button[2]')
            submitFilterButton.click()
            print("SET TO 2 YEARS ++++++++++++++++++++++++++++++++")

            time.sleep(2)
        except:
            print("No when sold dropdown detected")

def ScrapeMLS(currentPage, driver, STARTMLSDIG, ENDMLSDIG):
    ''' scrape the mls numbers off of the website '''
    # Variable Declaration
    keyword = ".png"
    MLSs = []

    while True:
        nextButton = driver.find_element_by_xpath(NEXTBUTTON)
        for element in currentPage.find_all('a', title=" Show full brochure ", ):
            if keyword in str(element):
                pass
            else:
                MLS = str(element)
                # extract mls from element.
                MLS = MLS[STARTMLSDIG:ENDMLSDIG]
                
                if MLS == '':
                    continue

                print(MLS)
            
                #add mls number to list.
                MLSs.append(MLS)
        
        # Cleanup variable- moving onto next page.
        MLS = ""
        
        # is next page button still there? if not, we've reached the last page.
        while True:
            testNextButton = driver.find_element_by_xpath(NEXTBUTTON)
            try:
                isNextButtonDisplayed = testNextButton.is_displayed()
                print(isNextButtonDisplayed)
                break
            except:
                print("Next button display check is broken, resetting definition...")

        if (isNextButtonDisplayed == True):
            print("keep flipping")
            #time.sleep(10)
            while True:
                try:
                    nextButton.click()
                    break
                except:
                    print("Next button is broken, resetting definition...")
                    nextButton = driver.find_element_by_xpath(NEXTBUTTON)
            driver.implicitly_wait(.3)
            #driver.switch_to.frame("searchFrame")
            currentPage=BeautifulSoup(driver.page_source, 'lxml')
        else:
            print("stop flipping")
            return MLSs
            break

def MLSToLink(MLSs, email):
    ''' converts mls to links '''
    listingLinks = []
    print("converting mls to link...")
    for mls in MLSs:
        listingLinks.append("https://www.zealty.ca/mls.php?id=" + mls + "&u=" + email + "&email=")
    return listingLinks

def main(browserOptions, browserPath):
    ''' scrapes mls numbers, converts to links and returns links. '''
    currentDT = datetime.datetime.now()
    STARTMLSDIG = 35
    ENDMLSDIG = -185
    listingLinks = []
    isLoggedIn = False

    #launch url
    mainUrl = "https://www.zealty.ca/search.html"
    
    driver = SetupBrowser(mainUrl, browserOptions, browserPath)

    # login and store email and password for future use
    credentials = Login(driver)
    isLoggedIn = True
    email = credentials[0]
    password = credentials[1]

    # select region and store region chosen
    geoAnswer = SelectRegion(driver)
    
    # Logout and continue. (fixes weird after ~100 pages no listing bug)
    Logout(isLoggedIn, driver)
    isLoggedIn = False

    # select date range
    SetSoldRange(driver)

    # set next button
    nextButton = driver.find_element_by_xpath(NEXTBUTTON); #set next page button

    #Selenium hands the page source to Beautiful Soup
    # Initial Page
    currentPage=BeautifulSoup(driver.page_source, 'lxml')

    MLSs = ScrapeMLS(currentPage, driver, STARTMLSDIG, ENDMLSDIG)
            
    #Done gathering mls numbers, add them to urls.
    listingLinks = MLSToLink(MLSs, email)

    driver.quit()
    print("done getting urls!")
    
    #return a list with region, listinglist
    return [geoAnswer, listingLinks]