import csv
import os
import shutil
from time import sleep

import pandas as pd
import traceback
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

parent = os.path.dirname(os.getcwd())

# <<REPLACE_ME>> with the download path of the Chrome browser
download_path = "/Users/serauysal/Desktop/data_analytics/Downloads"
prefs = {"download.default_directory": download_path}

chromeOptions = webdriver.ChromeOptions()
chromeOptions.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chromeOptions)
# REPLACE_ME if line above does not work
# driver = webdriver.Chrome(service=ChromeService(os.path.join(parent, 'Code', 'chromedriver.exe'))

output_folder = os.path.join(parent, 'Output')
linklist_path = os.path.join(parent, 'Input', 'Link_ID.csv')
iteration_path = os.path.join(parent, 'Code', 'Iteration.txt')


def create_folder_if_not_exists(folder_name):
    path = os.path.join(folder_name)
    if not os.path.exists(path):
        os.makedirs(path)


create_folder_if_not_exists(os.path.join(parent, "Output"))

links = []
with open(linklist_path, 'r') as csvf:
    urls = csv.reader(csvf)
    for url in urls:
        links.append(url)

start = 0
end = len(links)

for s in range(start, end, 1):
    with open(iteration_path, 'w') as f:
        f.write("last iteration: " + str(s) + " MP_ID: " + str(links[s]))

    print(s)
    url = ''.join(links[s])
    web = 'https://lop.parl.ca/sites/ParlInfo/default/en_CA/People/Profile?personId=' + url
    trial = 1
    while trial < 10:
        try:
            driver.get(web)
            sleep(1)
            print("Tell BS to parse " + str(url) + ". Trial " + str(trial))

            html_content = driver.page_source
            soup = BeautifulSoup(html_content, "html.parser")
            name = soup.find(id='PersonTitle').text.strip()
            trial = 10
        except:
            trial = trial + 1

    col1 = []
    col2 = []
    col1.append('Name')
    col2.append(name)

    left = soup.find(id='PersonInfo').find_all("label")
    for l in left:
        col1.append(l.text.strip().replace(";", ","))

    right = soup.find(id='PersonInfo').find_all("span")
    for r in right:
        r = r.text.strip().replace(";", ",")
        # Change: if MP is the current member, website contains it in the name section with ";", which would break the file structure
        # Therefore, we do not process these values
        if 'Current Member of' not in r:
            col2.append(r)
    df = pd.DataFrame(col1).T
    df2 = pd.DataFrame(col2).T
    frame = [df, df2]
    data = pd.concat(frame)

    data.to_csv(os.path.join(output_folder, 'MP_ID_' + str(url) + '.csv'), sep=";", mode="w", index=False, header=False)

    # Find element to Expand
    driver.find_element(By.XPATH, '//*[@id="PersonContent"]/div[5]/div/div/div/label[2]').click()
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, "html.parser")
    sleep(1)
    arr = []
    t1 = soup.find_all("table", {'class': 'dx-datagrid-table dx-datagrid-table-fixed'})
    for s in range(0, len(t1), 1):
        if len(t1[s].find_all("a")) > 0:
            arr.append(t1[s].find_all("a"))
    df = pd.DataFrame(arr)
    df.to_csv(os.path.join(output_folder, 'MP_ID_FED_' + str(url) + '.csv'), sep=";", mode="w", index=False,
              header=False)

    # Electoral History
    try:
        download = driver.find_element(By.XPATH, '//*[@id="gridCandidates"]/div/div[4]/div/div/div[3]/div[3]/div/div')
        download.click()
        sleep(1)
        oldfile = os.path.join(download_path, 'Parliamentarian Profile - Electoral History.xlsx')
        shutil.move(oldfile, os.path.join(output_folder, 'ElectoralHistory_' + str(url) + '.xlsx'))
    except:
        traceback.print_exc()
        print("No such section")

    # Federal experience:
    try:
        download = driver.find_element(By.XPATH,
                                       '//*[@id="gridFederalExperienceList"]/div/div[4]/div/div/div[3]/div[3]/div/div')
        download.click()
        sleep(1)
        oldfile = os.path.join(download_path, 'Parliamentarian Profile - Federal Experience without Parliament.xlsx')
        shutil.move(oldfile, os.path.join(output_folder, 'FederalExperience_' + str(url) + '.xlsx'))
    except:
        traceback.print_exc()
        print("No such section")
    # Committee:
    try:
        download = driver.find_element(By.XPATH,
                                       '//*[@id="gridCommitteeMembership"]/div/div[4]/div/div/div[3]/div[3]/div/div')
        download.click()
        sleep(1)
        oldfile = os.path.join(download_path, 'Parliamentarian Profile - Committee Membership.xlsx')
        shutil.move(oldfile, os.path.join(output_folder, 'Committee_' + str(url) + '.xlsx'))
    except:
        traceback.print_exc()
        print("No such section")

    # Provincial Experience
    try:
        download = driver.find_element(By.XPATH,
                                       '//*[@id="gridProvincialExperience"]/div/div[4]/div/div/div[3]/div[3]/div/div')
        download.click()
        sleep(1)
        oldfile = os.path.join(download_path, 'Parliamentarian Profile - Provincial Experience.xlsx')
        shutil.move(oldfile, os.path.join(output_folder, 'Province_' + str(url) + '.xlsx'))
    except:
        traceback.print_exc()
        print("No such section")

    # Family
    try:
        download = driver.find_element(By.XPATH, '//*[@id="gridFamily"]/div/div[4]/div/div/div[3]/div[3]/div/div')
        download.click()
        sleep(1)

        oldfile = os.path.join(download_path, 'Parliamentarian Profile - Family Ties in Parliament.xlsx')
        shutil.move(oldfile, os.path.join(output_folder, 'Family_' + str(url) + '.xlsx'))
    except:
        traceback.print_exc()
        print("No such section")

    # Municipal
    try:
        download = driver.find_element(By.XPATH,
                                       '//*[@id="gridMunicipalExperience"]/div/div[4]/div/div/div[3]/div[3]/div/div')
        download.click()
        sleep(1)
        oldfile = os.path.join(download_path, 'Parliamentarian Profile - Municipal Experience.xlsx')
        shutil.move(oldfile, os.path.join(output_folder, 'Municipal_' + str(url) + '.xlsx'))
    except:
        traceback.print_exc()
        print("No such section")

print("Done")