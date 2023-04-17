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
from sqlalchemy.orm import sessionmaker

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
Session = sessionmaker(bind=engine)
session = Session()
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
    def wait_for_element(self, by, value, wait=10):
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
    home = None
    away = None
    away_city = None
    home_city = None

    metadata = MetaData()
    my_table = Table('game_data_url', metadata, autoload_with=engine)

    with engine.connect() as conn:
        records = conn.execute(my_table.select()).fetchall()
        for record in records:
            if record.url:
                # url = 'https://www.nfl.com/games/chiefs-at-raiders-2022-reg-18?active-tab=watch'
                url = record.url
                try:
                    gs.navigate_to_url(url)

                    # close cookies
                    try:
                        gs.select_element(By.XPATH, '//*[@id="onetrust-close-btn-container"]/button').click()
                    except:
                        pass

                    # First, we need to get all the data from all available quarters
                    gs.wait_for_element(By.XPATH, '//*[@id="all-drives-panel"]')
                    quarter_elements = gs.select_element(By.XPATH, '//*[@id="all-drives-panel"]').find_elements(By.XPATH,
                                                                                                                './*')

                    cities = gs.select_element(By.XPATH, '//*[@class="css-1je2xdb"]').find_elements(By.XPATH, './*')
                    p = 0
                    for city in cities:
                        if city:
                            soup = BeautifulSoup(city.get_attribute('innerHTML'), 'html.parser')
                            city = soup.find_all('a', class_='css-lu4k2s')
                            print(city, len(city))
                            if len(city) > 0:
                                if p == 0:
                                    home_city = city[0].text
                                    home_city = home_city.split("(")
                                    print(home_city)
                                else:
                                    away_city = city[0].text
                                    away_city = away_city.split("(")
                                    print(away_city)
                                p = p + 1

                    teams = gs.select_element(By.XPATH, '//*[@class="css-1je2xdb"]').find_elements(By.XPATH, './*')
                    g = 0
                    for team in teams:
                        if team:
                            soup = BeautifulSoup(team.get_attribute('innerHTML'), 'html.parser')
                            team = soup.find_all('div', class_='css-f5k9xp')
                            print(team, len(team))
                            if len(team) > 0:
                                if g == 0:
                                    home = team[0].text
                                    home = home.split("(")
                                    print(home)
                                else:
                                    away = team[0].text
                                    away = away.split("(")
                                    print(away)
                                g = g + 1

                    j = 0
                    i = 1
                    # iterate through quarters
                    for quarter in quarter_elements:
                        # get all games
                        # NOTE: I used classes as selectors, if the website changes in the future,
                        #       this code will not work

                        quarter_name = quarter.find_element(By.CLASS_NAME, 'css-a45wr7').text
                        print(quarter_name)
                        games = quarter.find_elements(By.CLASS_NAME, 'css-7w6khc')

                        # get all data from games
                        for game in games:
                            # Get header and information
                            main_header = game.find_element(By.CSS_SELECTOR, 'div[role="button"]')
                            actions.move_to_element(main_header).perform()
                            wait = WebDriverWait(gs.driver, 10)
                            wait.until(EC.visibility_of(main_header))

                            main_header.click()
                            header_name = main_header.find_element(By.CSS_SELECTOR,
                                                                   'div.css-view-175oi2r.r-flex-dta0w2.r-flexDirection-18u37iz.r-maxWidth-146iojx.r-minWidth-ek4qxl.r-paddingHorizontal-1j3t67a').text
                            main_information = main_header.find_elements(By.CSS_SELECTOR,
                                                                         'div.css-view-175oi2r.r-flex-6wfxan.r-flexDirection-18u37iz.r-justifyContent-a2tzq0.r-width-13qz1uu > div.css-view-175oi2r.r-paddingLeft-1qhn6m8')
                            information_data = [i.text.split('\n')[0] for i in main_information]

                            # get game event information
                            soup = BeautifulSoup(game.get_attribute('innerHTML'), 'html.parser')
                            event_timelines = soup.find_all('div', tabindex="0", class_='css-1cd8nnu')

                            for timeline in event_timelines:
                                j = j + 1
                                timeline_data = {}

                                current_down = timeline.find('div', dir='auto',
                                                             class_='css-text-1rynq56 r-color-1khnkhu r-fontFamily-1ujtvat r-fontSize-ubezar')
                                description = timeline.find('div', dir='auto',
                                                            class_='css-text-1rynq56 r-color-zyhucb r-fontFamily-1fpbnck r-fontSize-1b43r93 r-lineHeight-hbpseb r-marginTop-1bymd8e r-paddingBottom-xd6kpl')
                                distance = timeline.find('div', dir='auto',
                                                         class_='css-text-1rynq56 r-color-zyhucb r-fontFamily-1rof6co r-fontSize-1enofrn r-marginTop-l71dzp')

                                timeline_data['gameid'] = record.gameid
                                timeline_data['game_name'] = str(home[0]) + "" + str(home_city[0]) + " at " + str(
                                    away[0]) + "" + str(
                                    away_city[0])
                                timeline_data['home_team'] = away[0] + "" + str(away_city[0])
                                timeline_data['away_team'] = home[0] + "" + str(home_city[0])
                                timeline_data['quarter'] = quarter_name[0]
                                timeline_data['drive_number'] = i
                                timeline_data['play_number'] = j

                                u_down = distance.text
                                print(u_down)
                                u_down = u_down.split("at")
                                timeline_data['down'] = u_down[0]
                                timeline_data['play'] = current_down.text
                                game_time = None
                                if description.text[0] == "(":
                                    m_desc = description.text
                                    m_desc = m_desc.split(")", 1)
                                    game_time = m_desc[0]
                                    m_desc = m_desc[1]

                                else:
                                    m_desc = description.text
                                    game_time = ''

                                timeline_data['play_description'] = m_desc
                                if game_time != '':
                                    game_time = game_time.replace("(", "")
                                    game_time = game_time.split(":")
                                    if game_time[0] == '':
                                        game_time[0] = 0

                                    try:
                                        timeline_data['game_time'] = str(game_time[0]) + ":" + game_time[1]
                                    except:
                                        timeline_data['game_time'] = game_time
                                else:
                                    timeline_data['game_time'] = ''

                                data = pd.json_normalize(timeline_data)

                                table_name = 'game_solo_details'
                                data.to_sql(table_name, engine, if_exists='append', index=False)

                                # game_data_detail.append(timeline_data)

                            i = i + 1
                except:
                    pass

def work():
    main()
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d-%H-%M-%S")


if __name__ == '__main__':
    # scheduler = BlockingScheduler()
    #
    # # scrap every 30 seconds
    # scheduler.add_job(work, 'interval', seconds=60)
    # scheduler.start()
    main()
    pass
