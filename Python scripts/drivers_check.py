## This script helps to automatically parse data from TfL (Transport of London).
## Inputs: Driver's licence number
## Outputs: License expiry date

import os
import os.path

# specify below the path to your folder with 'chromedriver' file
os.environ["PATH"] += os.pathsep + r'/repositories/analytics/'

from selenium import webdriver
from selenium.webdriver.common import action_chains, keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
import time
import csv
import pandas as pd

driver = webdriver.Chrome('../chromedriver')
# driver = webdriver.Chrome('chromedriver')

import_files_folder = 'drivers' # folder with drivers1.txt, drivers2.txt etc. - files with lists of drivers
export_filename = 'drivers_info.csv'

def nonblank_lines(f):
    for l in f:
        line = l.rstrip()
        if line:
            yield line
            
for filename in sorted(os.listdir(import_files_folder)):
    if filename.endswith(".txt"):
        with open(import_files_folder + '/' + filename, "r") as ins:
            print('Processing {}...'.format(filename))
            for line in nonblank_lines(ins):
                driver.get("https://tph.tfl.gov.uk/TfL/SearchDriverLicence.page?org.apache.shale.dialog.DIALOG_NAME=TPHDriverLicence&Param=lg2.TPHDriverLicence&menuId=6")
                licenceNumber = driver.find_element_by_id("searchdriverlicenceform:DriverLicenceNo")
                licenceNumber.send_keys(line)
                driver.find_element_by_name("searchdriverlicenceform:_id189").click()

                check = driver.find_elements_by_id("main-content")
                if check[0].text.find('records') != -1:
                    print(line + '\t' + 'TO CHECK')
                else:
                    time.sleep(2)
                    array = []
                    ele = driver.find_elements_by_xpath("//*[@id='_id177:driverResults:tbody_element']/tr")

                    for e in ele:
                        for td in e.find_elements_by_xpath(".//td"):
                            #print td.text
                            array.append(td.text)

        #             print(array[0] + '\t' + array[1] + '\t' + array[2] )
                    driver_info = [array[0], array[1], array[2]]
                    df = pd.DataFrame([driver_info])

                    if os.path.isfile(export_filename):
                        # if file already exists, then append row
                        df.to_csv(export_filename, index=False, mode='a', header=False)
                    else:
                        # if file not exists
                        df.columns = ['Licence Number'
                                     ,'Licence Holder Name'
                                     ,'Licence Expiry Date']
                        df.to_csv(export_filename, index=False)
            print('{} processed successfully. Data exported to {}'.format(filename, export_filename))

print('Finished. All data exported to:', export_filename)

driver.quit()