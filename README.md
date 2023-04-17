# NFL Scrapper

NFL Scrapper is a website scraper. It scrap data from https://www.nfl.com/

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install NFL Scrapper.

```bash
pip install requirements.txt
```

Install Postgres, Configure/Update db configuration in url.py, data.py
To-Do: I will add db config file later
```bash
conn_params = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "**********",
    "sslmode": "prefer",
    "connect_timeout": "10"
}
```

## Usage
# python url.py
```python
First, run url.py
1. urls.py is responsible for extracting (year and week-wise) urls.
2. In addition there is separate logic for 18,19,20,21,22 weeks as requested by the client.
3. Overall urls.py will extract (gameid, home, away, year, week, url) from the site and store it in DB
```

# python data.py
```python
Second, run data.py
1. it will take information from stored file (DB/Table) which contains game urls and other information
2. visit every url
3. extract all the details like (gameid, game_name, home_team, away_team, quarter, drive_number, play_number, down, play, play_description, game_time)
4. Stores it in db
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
