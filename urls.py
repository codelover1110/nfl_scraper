import time

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
import logging as log
import json
from bs4 import BeautifulSoup

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from datetime import datetime

from sqlalchemy import create_engine, MetaData, Table
import pandas as pd
import psycopg2

log.basicConfig(level=log.INFO)

# Set up the connection parameters
conn_params = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "Foryoureyesonly11",
    "sslmode": "prefer",
    "connect_timeout": "10"
}

# Connect to the database
conn = psycopg2.connect(**conn_params)
engine = create_engine(
    f'postgresql://{conn_params["user"]}:{conn_params["password"]}@{conn_params["host"]}:{conn_params["port"]}/{conn_params["database"]}')

# Create a cursor object
cur = conn.cursor()


# initial setup of selenium
class GameScrap:
    def __init__(self):
        self.driver = None
        self.actions = None
        pass

    # set up driver
    def configure_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        self.driver = webdriver.Chrome(options=options)
        self.actions = ActionChains(self.driver)

    # function to wait for an element to be visible on the page before performing an action
    def wait_for_element(self, by, value, wait=20):
        wait = WebDriverWait(self.driver, wait)
        element = wait.until(EC.visibility_of_element_located((by, value)))
        return element

    # function to navigate to a URL
    def navigate_to_url(self, url):
        self.driver.get(url)

    # function to select an element by a specific selector
    def select_element(self, by, value):
        element = self.driver.find_element(by, value)
        return element


game_data_detail = []
# driver setup
gs = GameScrap()
gs.configure_driver()
actions = gs.actions


def main():
    global game_data_detail
    game_data_detail = []
    gameid = 0


    metadata = MetaData()
    my_table = Table('nfl_url_pre', metadata, autoload_with=engine)
    with engine.connect() as conn:
        records = conn.execute(my_table.select()).fetchall()
        for record in records:
            print(record.url)
            gs.navigate_to_url(record.url)
            time.sleep(15)
            # close cookies
            try:
                gs.select_element(By.XPATH, '//*[@id="onetrust-close-btn-container"]/button').click()
            except:
                pass

            # First, we need to get all the data from all available quarters
            gs.wait_for_element(By.XPATH, './/div[@class="css-37urdo"]')
            match_elements = gs.select_element(By.XPATH, './/div[@class="css-37urdo"]').find_elements(By.XPATH,
                                                                                                      './/div[@class="css-156uxf7"]')
            try:
                # iterate through quarters
                for match in match_elements:
                    nfl_url_data = {}
                    g_dt_final = None

                    soup = BeautifulSoup(match.get_attribute('innerHTML'), 'html.parser')
                    infos = soup.find_all('div',
                                          class_='css-text-1rynq56 r-color-1khnkhu r-fontFamily-1fdbu1n r-fontSize-ubezar')

                    game_date = soup.find('div', class_ = 'css-text-1rynq56 r-color-1khnkhu r-fontFamily-1fdbu1n r-fontSize-1enofrn r-lineHeight-1cwl3u0 r-paddingLeft-m2pi6t r-textAlign-q4m81j')
                    g_dt = str(game_date)
                    # print(g_dt)
                    # game_date = game_date[1]
                    if game_date:
                        game_date = g_dt.split(">")
                        game_d = game_date[1].split("<")
                        game_d = game_d[0].split(' ')
                        game_day = game_d[0]
                        game_month = game_d[1].split('/')
                        g_dt_final = str(record.nfl_year)+"-"+str(game_month[0])+"-"+str(game_month[1])
                        # print("Game Date ======================> ", g_dt_final)

                    if infos:
                        # print("infos", infos)
                        gameid = gameid + 1
                        home = infos[0].text
                        away = infos[1].text

                        nfl_url_data['gameid'] = gameid
                        wn = str(record.week_name)
                        if wn.startswith('Week'):
                            nfl_url_data['url'] = "https://www.nfl.com/games/" + str(home.lower()) + "-at-" + str(
                                away.lower()) + "-" + str(record.nfl_year) + "-reg-" + str(record.week_no)
                        elif wn.startswith('Pro'):
                            nfl_url_data['url'] = "https://www.nfl.com/games/" + str(home.lower()) + "-at-" + str(
                                away.lower()) + "-" + str(record.nfl_year) + "-pro-" + str(record.week_no)
                        elif wn.startswith('Preseason'):
                            nfl_url_data['url'] = "https://www.nfl.com/games/" + str(home.lower()) + "-at-" + str(
                                away.lower()) + "-" + str(record.nfl_year) + "-pre-" + str(record.week_no)
                        elif wn.startswith('Hall'):
                            nfl_url_data['url'] = "https://www.nfl.com/games/" + str(home.lower()) + "-at-" + str(
                                away.lower()) + "-" + str(record.nfl_year) + "-pre-" + str(record.week_no)
                        else:
                            nfl_url_data['url'] = "https://www.nfl.com/games/" + str(home.lower()) + "-at-" + str(
                                away.lower()) + "-" + str(record.nfl_year) + "-post-" + str(record.week_no)
                        nfl_url_data['home'] = home
                        nfl_url_data['away'] = away
                        nfl_url_data['yr'] = record.nfl_year
                        nfl_url_data['week_no'] = record.week_no
                        nfl_url_data['date'] = g_dt_final
                        nfl_url_data['week_name'] = record.week_name
                        print(nfl_url_data)
                        #game_data_detail.append(nfl_url_data)

                        data = pd.json_normalize(nfl_url_data)
                        # print(data)
                        table_name = 'nfl_game_urls-v1'
                        data.to_sql(table_name, engine, if_exists='append', index=False)
            except Exception as e:
                print(e)
                pass



    # for yr in range(2017, year):
    #     if yr <= 2020:
    #         for week in range(1, 18):
    #             url = 'https://www.nfl.com/scores/' + str(yr) + '/REG' + str(week)
    #             print(url)
    #             gs.navigate_to_url(url)
    #             time.sleep(20)
    #             # close cookies
    #             try:
    #                 gs.select_element(By.XPATH, '//*[@id="onetrust-close-btn-container"]/button').click()
    #             except:
    #                 pass
    #
    #             # First, we need to get all the data from all available quarters
    #             gs.wait_for_element(By.XPATH, './/div[@class="css-37urdo"]')
    #             match_elements = gs.select_element(By.XPATH, './/div[@class="css-37urdo"]').find_elements(By.XPATH,
    #                                                                                                       './/div[@class="css-156uxf7"]')
    #             try:
    #                 # iterate through quarters
    #                 for match in match_elements:
    #                     nfl_url_data = {}
    #                     soup = BeautifulSoup(match.get_attribute('innerHTML'), 'html.parser')
    #                     infos = soup.find_all('div',
    #                                           class_='css-text-1rynq56 r-color-1khnkhu r-fontFamily-1fdbu1n r-fontSize-ubezar')
    #                     print("infos", infos)
    #                     if infos:
    #                         gameid = gameid + 1
    #                         home = infos[0].text
    #                         away = infos[1].text
    #
    #                         nfl_url_data['gameid'] = gameid
    #                         nfl_url_data['url'] = "https://www.nfl.com/games/" + str(home.lower()) + "-at-" + str(
    #                             away.lower()) + "-" + str(yr) + "-reg-" + str(week)
    #                         nfl_url_data['home'] = home
    #                         nfl_url_data['away'] = away
    #                         nfl_url_data['year'] = yr
    #                         nfl_url_data['week'] = week
    #                         print(nfl_url_data)
    #                         game_data_detail.append(nfl_url_data)
    #             except:
    #                 pass
    #     else:
    #         for week in range(1, 19):
    #             url = 'https://www.nfl.com/scores/' + str(yr) + '/REG' + str(week)
    #             print(url)
    #             gs.navigate_to_url(url)
    #             time.sleep(20)
    #             # close cookies
    #             try:
    #                 gs.select_element(By.XPATH, '//*[@id="onetrust-close-btn-container"]/button').click()
    #             except:
    #                 pass
    #
    #             # First, we need to get all the data from all available quarters
    #             gs.wait_for_element(By.XPATH, './/div[@class="css-37urdo"]')
    #             match_elements = gs.select_element(By.XPATH, './/div[@class="css-37urdo"]').find_elements(By.XPATH,
    #                                                                                                       './/div[@class="css-156uxf7"]')
    #             try:
    #                 # iterate through quarters
    #                 for match in match_elements:
    #                     nfl_url_data = {}
    #                     soup = BeautifulSoup(match.get_attribute('innerHTML'), 'html.parser')
    #                     infos = soup.find_all('div',
    #                                           class_='css-text-1rynq56 r-color-1khnkhu r-fontFamily-1fdbu1n r-fontSize-ubezar')
    #                     print("infos", infos)
    #                     if infos:
    #                         gameid = gameid + 1
    #                         home = infos[0].text
    #                         away = infos[1].text
    #
    #                         nfl_url_data['gameid'] = gameid
    #                         nfl_url_data['url'] = "https://www.nfl.com/games/" + str(home.lower()) + "-at-" + str(
    #                             away.lower()) + "-" + str(yr) + "-reg-" + str(week)
    #                         nfl_url_data['home'] = home
    #                         nfl_url_data['away'] = away
    #                         nfl_url_data['year'] = yr
    #                         nfl_url_data['week'] = week
    #                         print(nfl_url_data)
    #                         game_data_detail.append(nfl_url_data)
    #             except:
    #                 pass
    #
    # for yr in range(2017, year):
    #     for week in range(1, 5):
    #         url = 'https://www.nfl.com/scores/' + str(yr) + '/POST' + str(week)
    #
    #         if week == 1:
    #             weeek = 19
    #         elif week == 2:
    #             weeek = 20
    #         elif week == 3:
    #             weeek = 21
    #         elif week == 4:
    #             weeek = 22
    #
    #
    #         print(url)
    #         gs.navigate_to_url(url)
    #         time.sleep(20)
    #         # close cookies
    #         try:
    #             gs.select_element(By.XPATH, '//*[@id="onetrust-close-btn-container"]/button').click()
    #         except:
    #             pass
    #
    #         # First, we need to get all the data from all available quarters
    #         gs.wait_for_element(By.XPATH, './/div[@class="css-37urdo"]')
    #         match_elements = gs.select_element(By.XPATH, './/div[@class="css-37urdo"]').find_elements(By.XPATH,
    #                                                                                                   './/div[@class="css-156uxf7"]')
    #         try:
    #             # iterate through quarters
    #             for match in match_elements:
    #                 nfl_url_data = {}
    #                 soup = BeautifulSoup(match.get_attribute('innerHTML'), 'html.parser')
    #                 infos = soup.find_all('div',
    #                                       class_='css-text-1rynq56 r-color-1khnkhu r-fontFamily-1fdbu1n r-fontSize-ubezar')
    #                 print("infos", infos)
    #                 if infos:
    #                     gameid = gameid + 1
    #                     home = infos[0].text
    #                     away = infos[1].text
    #
    #                     nfl_url_data['gameid'] = gameid
    #                     nfl_url_data['url'] = "https://www.nfl.com/games/" + str(home.lower()) + "-at-" + str(
    #                         away.lower()) + "-" + str(yr) + "-post-" + str(week)
    #                     nfl_url_data['home'] = home
    #                     nfl_url_data['away'] = away
    #                     nfl_url_data['year'] = yr
    #                     nfl_url_data['week'] = weeek
    #                     print(nfl_url_data)
    #                     game_data_detail.append(nfl_url_data)
    #         except:
    #             pass


def work():
    main()
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d-%H-%M-%S")

    # data = pd.json_normalize(game_data_detail)
    #
    # table_name = 'nfl_url_details'
    # data.to_sql(table_name, engine, if_exists='replace', index=False)


if __name__ == '__main__':
    work()
    # scheduler = BlockingScheduler()
    #
    # # scrap every 30 seconds
    # scheduler.add_job(work, 'interval', seconds=10)
    # scheduler.start()
    pass
