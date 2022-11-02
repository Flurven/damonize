from sys import maxsize
import selenium.webdriver as webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
from datetime import date
import pandas as pd
import bs4
from prefect import flow, task
#from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import cloud_storage_upload_blob_from_file
from prefect_gcp.bigquery import bigquery_load_cloud_storage


@task
def page_numbers(browser, first_page):
    browser.get(first_page)
    soup = bs4.BeautifulSoup(browser.page_source, features="html.parser")
    i = 0
    page_number = [1]
    while page_number != []:
        page_number = soup.select(f"#ctl00_ctl01_DefaultSiteContentPlaceHolder1_Col1_ucNavBar_rptButtons_ctl0{i}_btnPage")
        if page_number != []:
            page = page_number[0].text
        i=i+1
    return page

@task
def get_link(browser, page_numbers, first_page):
    objects = []
    browser.get(first_page)

    soup = bs4.BeautifulSoup(browser.page_source, features="html.parser")

    links = []
    for p in range(0, int(page_numbers)):
        soup = bs4.BeautifulSoup(browser.page_source, features="html.parser")
       # button = browser.find_element_by_id("ctl00_ctl01_DefaultSiteContentPlaceHolder1_Col1_ucNavBar_btnNavNext")
        button = browser.find_element(By.ID, "ctl00_ctl01_DefaultSiteContentPlaceHolder1_Col1_ucNavBar_btnNavNext")
        grids = soup.select(".gridcell")
        for item in grids:
            for a in item.find_all('a'):
                if a['href'][:13] == 'ObjectDetails':
                    links.append(a['href'])
        button.click()
    for l in links:
        objects.append('https://minasidor.lkf.se/HSS/Object/'+l)
    return objects

@task
def get_objects(browser, objects):
    object_list = []
      
    ### Iterate over list of links to extract data on each object
    for o in objects:
        browser.get(o)
        #time.sleep(30)
        soup = bs4.BeautifulSoup(browser.page_source, features="html.parser")
        #time.sleep(2)
        if soup.select("#ctl00_ctl01_lblTitle")[0].text == 'Lägenhet ej tillgänglig':
            break
        ruta2 = soup.select("#ctl00_ctl01_DefaultSiteContentPlaceHolder1_Col1_divAriaFacts")
        ruta1 = soup.select("#ctl00_ctl01_DefaultSiteContentPlaceHolder1_Col1_divAvailabilityBoxTop")
        #time.sleep(2)
        datalista1 = ruta1[0].text.split("\n")[3:-2]
        datalista2 = ruta2[0].text.split("\n")[4:-2]
        datalista1.extend(datalista2)
        res = [ele for ele in datalista1 if ele != ""]
        if "Nybyggnation" in res:
            res = [ele for ele in res if ele != "Nybyggnation"]
            res.extend(["Annonstyp", "Nybyggnation"])
        if "Korttidskontrakt" in res:
            res = [ele for ele in res if ele != "Korttidskontrakt"]
            res.extend(["Annonstyp", "Korttidskontrakt"])
        if "Snabb inflyttning. Tänk på din egen uppsägningstid i ditt nuvarande boende." in res:
            res = [ele for ele in res if ele != "Snabb inflyttning. Tänk på din egen uppsägningstid i ditt nuvarande boende."]
            res.extend(["Annonstyp", "Snabb inflyttning"])
        if "Hyrs ut till de som är 55 år eller äldre, som saknar hemmavarande barn." in res:
            res = [ele for ele in res if ele != "Hyrs ut till de som är 55 år eller äldre, som saknar hemmavarande barn."]
            res.extend(["Annonstyp", "55+"])
        if "Radhus" in res:
            res = [ele for ele in res if ele != "Radhus"]
            res.extend(["Annonstyp", "Radhus"])
        if "Boende med tyngdpunkt på tillgänglighet. Till för personer 65 år eller äldre." in res:
            res = [ele for ele in res if ele != "Boende med tyngdpunkt på tillgänglighet. Till för personer 65 år eller äldre."]
            res.extend(["Annonstyp", "65+"])
        object_data = {}
        for i in range(0,len(res),2):
            object_data[res[i]]=res[i+1]
        object_list.append(object_data)
    
    browser.close()
    return object_list

@task
def objects_to_csv(filename, folder, object_list):
 
    ### write result to file

    df = pd.DataFrame(object_list)
    df['source'] = 'lkf.se'
    df['extract_date'] = date.today()
    df.to_csv(folder+filename, sep = '\t', encoding = 'utf-8', header = 'true')
    return df

@flow
def lkf_extract():
    lkf_in = ('https://minasidor.lkf.se/HSS/Object/object_list.aspx?cmguid=4e6e781e-5257-403e-b09d-7efc8edb0ac8&objectgroup=1&action=Available')
    file = "{}{}{}".format('LKF_objects_', date.today(), '.csv')
    buck = 'lkf_source'
    local_folder = 'lkf/'
    file_uri = "{}{}{}{}".format('gs://', buck, '/', file)
    gcp_credentials = GcpCredentials(service_account_file="/home/pi/Downloads/damonize-dev-0f40b1a42037.json")

    browser = webdriver.Chrome()
    
    pn = page_numbers(browser, first_page=lkf_in)
    obj_links = get_link(browser=browser, page_numbers=pn, first_page=lkf_in)
    obj_list = get_objects(browser=browser, objects=obj_links)
    a = objects_to_csv(filename=file, folder=local_folder, object_list=obj_list)
  
    blob = cloud_storage_upload_blob_from_file(local_folder+file, buck, file, gcp_credentials, wait_for=[a])

    conf = dict(field_delimiter='\t')
    result = bigquery_load_cloud_storage(
        dataset="stg",
        table="test_table",
        uri=file_uri,
        gcp_credentials=gcp_credentials,
        location = 'europe-north1',
        job_config = conf, 
        wait_for=[blob]
    )
    

if __name__ == '__main__':
    a = lkf_extract()
    