# set up a continuous crawling pipeline
from selenium import webdriver 
from time import sleep 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
from pprint import pprint
from urllib.parse import unquote, urlparse
import json
import os
from datetime import datetime, timedelta
import zipfile
import mysql.connector
import glob
import pandas as pd
import gzip

DRIVER_PATH = os.getenv('CHROME_DRIVER_PATH')
DOWNLOAD_PATH = '/data/zc1245/gateway_crawl/new_log_endpoint_03052021/'


# reference: https://stackoverflow.com/questions/34338897/python-selenium-find-out-when-a-download-has-completed
def download_wait(directory, timeout, nfiles=None):
    """
    Wait for downloads to finish with a specified timeout.

    Args
    ----
    directory : str
        The path to the folder where the files will be downloaded.
    timeout : int
        How many seconds to wait until timing out.
    nfiles : int, defaults to None
        If provided, also wait for the expected number of files.

    """
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < timeout:
        time.sleep(1)
        dl_wait = False
        files = os.listdir(directory)
        if nfiles and len(files) != nfiles:
            dl_wait = True

        for fname in files:
            if fname.endswith('.crdownload'):
                dl_wait = True

        seconds += 1
    return seconds


def get_csv_file():
    files = glob.glob("{}/*.csv".format(DOWNLOAD_PATH))
    if len(files) > 1:
        print('too many csvs -- got {} files'.format(len(files)))
    return sorted(files, reverse=True)[0]


def transform_csv_to_mysql(filename):
    print('current filename is {}'.format(filename))

    df_example = pd.read_csv(filename, index_col=False, sep=',', encoding = "ISO-8859-1", quotechar='"', error_bad_lines=False, engine='python')
    df_example = df_example.where(pd.notnull(df_example), None)

    USER = 'root'
    PASSWORD = 'csmap'
    HOST = 'localhost'
    PORT = '3306'
    DATABASE = 'webtraffic'

    SELECT_QUERY = 'SELECT count(*) FROM gateway_02032021 LIMIT 10'
    UPDATE_QUERY = """
      INSERT IGNORE INTO gateway_02032021
      (date_and_time,
      browser, 
      os, 
      resolution, 
      ip_address, 
      version,
      country,
      region,
      city,
      postal_code,
      isp,
      type,
      page_title,
      url,
      came_from, 
      returning_count) 
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      """

    # establish the connection
    cnx = mysql.connector.connect(user=USER, password=PASSWORD,
                                  host=HOST,
                                  port=PORT,
                                  database=DATABASE, 
                                  charset='utf8',
                                  use_unicode=True)
    cursor = cnx.cursor()

    for index, row in df_example.iterrows():
        try:
            cursor.execute(UPDATE_QUERY,
                       (row['Date and Time'], row['Browser'], row['OS'], row['Resolution'],
                        row['IP Address'], row['Version'], row['Country'], 
                        row['Region'], row['City'], row['Postal Code'], row['ISP'], 
                        row['Type'], row['Page Title'], row['URL'],
                        row['Came From'], row['Returning Count'])
                      )
        except Exception as e:
            print('[Mysql execute Exceptionn!!]')
            print(e)
            pass

        """NOTE: 
               To minimize disk I/O operation, we only write to the disk for every 1000 records    

               Execute and commit are very different, see more here
               https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-transaction.html
        """
        if index % 1000 == 0:
            cnx.commit()
            print("index {} || {} record(s) affected".format(index, cursor.rowcount))


    # commit last batch of records from the for loop
    cnx.commit()

    # close the connection
    cursor.close()
    cnx.close()
    
    # zip the file after the transformation
    # zipfile.ZipFile(filename.replace('.csv', '.zip'), mode='w', compression=zipfile.ZIP_DEFLATED).write(filename)
    print('convert to gz')
   
    with open(filename, 'rb') as f_in, gzip.open(filename.replace('.csv', '.gz'), 'wb') as f_out:
        f_out.writelines(f_in)
    print('remove .csv file') 
    try:
        os.remove(filename)
    except OSError:
        print('cannot remove the file!')
        pass
    return


def main_crawler():
    driver = None
    broswer_log = 'chrome_browser.log'
    try: 
        chrome_options = Options()
        
        chrome_options.add_argument('window-size=1200x600')
        # TODO: if you do not want to `see` the browser working, use --headless argument, uncomment the following line
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        # chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_experimental_option('w3c', False)

        prefs = {'download.default_directory' : DOWNLOAD_PATH}
        chrome_options.add_experimental_option('prefs', prefs)

        
        # chrome_options.add_argument('--proxy-server=%s:%s' % (proxy_host, proxy_port))

        caps = DesiredCapabilities().CHROME
        caps["pageLoadStrategy"] = "none" 
        caps["loggingPrefs"] = {'performance': 'ALL', 'browser':'ALL', 'network':'ALL'}  # log more events (network, timeline)   

        # TOOD: logging is turned off, as it's not very necessary
        driver = webdriver.Chrome(
            executable_path=DRIVER_PATH,
            chrome_options=chrome_options, 
            #desired_capabilities=caps,
            #service_args=["--verbose", "--log-path={0}".format(broswer_log)],
        )
          
        driver.get('https://statcounter.com/p9449268/?guest=1')
        time.sleep(4)
            
        finish_crawl = False
        num_files = 0
        while num_files <= 4 * 24 * 340:
            if num_files >= 1500 and num_files % 1500 == 0:
                print('need to update the driver...')
                driver.quit()
                driver = None
                driver = webdriver.Chrome(
                    executable_path=DRIVER_PATH,
                    chrome_options=chrome_options,
                )

                driver.get('https://statcounter.com/p9449268/?guest=1')
                time.sleep(8)


            print('current num_files is {}'.format(num_files))
            time_now = datetime.now()
            print('current time is {}'.format(time_now))
            end_time = (time_now - timedelta(hours=1)).isoformat()[:-7] 
            start_time = (time_now - timedelta(hours=33)).isoformat()[:-7]
            log_url = 'https://statcounter.com/p9449268/csv/download_log_file?range={}--{}'.format(start_time, end_time)
            print(log_url)
            driver.get(log_url)
            #driver.get('https://statcounter.com/p9449268/pageload/?csv')
            
            # wait for download to finish, 6 minutes 
            time.sleep(60 * 6)
            #download_wait(directory=DOWNLOAD_PATH, timeout=60, nfiles=None)
            num_files += 1
            try:
                transform_csv_to_mysql(get_csv_file())
            except Exception as e:
                print(e)
                pass
            # wait for remaining seconds (15min - time elapsed)
            time_elapsed = (datetime.now() - time_now).total_seconds()
            time_remaining = 60 * 15 - time_elapsed
            if time_remaining > 0:
                print('time remaining is {} seconds...'.format(time_remaining))
                time.sleep(time_remaining)
            
        if driver is not None:
            driver.quit()
            driver = None


    except Exception as e:
        if driver is not None:
            driver.quit()
        print('cannot get cookie! something is very wrong!')
        print(e)
        return None


if __name__ == '__main__':
    import argparse
    from datetime import datetime
    parser = argparse.ArgumentParser(description='build tweet tree.')
    parser.add_argument('--minute', type=str, default=None, required=False,
                       help='start the program at X minute')
    args = parser.parse_args()
    
    # use the new endpoint to download log
    # transform_csv_to_mysql(get_csv_file())
    main_crawler()
    exit(0) 

    # datetime object containing current date and time
    now = datetime.now()
    print("now =", now)

    # dd/mm/YY H:M:S
    dt_string = now.strftime("%Y-%m-%d")
    DOWNLOAD_PATH = '/data/zc1245/gateway_crawl/{}_page_{}_start/'.format(dt_string, args.minute)
    
    import os
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)

    while int(now.minute) != int(args.minute):
        now = datetime.now()
        print(int(now.minute), int(args.minute))
        time.sleep(30)
    print('current time is {}')
    print('program starts ')
    extract_cookie()


