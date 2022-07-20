import schedule
import datetime
import database
import api


def job():
    now = datetime.datetime.now()
    if conn & now.hour > 7 & now.hour < 23:
        json = api.poitier()
        json_cleaning = api.cleaning_poitier(json)
        database.insert(conn, json_cleaning, now)
        print("Insert data : {now}".replace('{now}', str(now)))


conn = database.get_connection()
schedule.every(20).minutes.do(job)

while True:
    schedule.run_pending()

