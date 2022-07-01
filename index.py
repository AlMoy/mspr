from requests import Session
import datetime
import psycopg2


def create_server_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            database='d6d2di5urm8k4',
            user='tlluztgglrdqjh',
            password='074e713f9585c4b95f3be3ff1339a04f52f78ceeb9de27779d697137114306d5',
            host='ec2-52-212-228-71.eu-west-1.compute.amazonaws.com',
            port='5432'
        )
        print('PostgresSQL Database connection successful')
    except psycopg2.Error as err:
        print(f'PostgresSQL Database connection failed : {err}')

    return conn


def get_data():
    dataset = 'mobilites-stationnement-des-parkings-en-temps-reel'
    lang = 'fr'
    timezone = 'Europe%2FParis'
    base_url = 'https://data.grandpoitiers.fr/api'
    search = f'{base_url}/records/1.0/search/?dataset={dataset}&q=&lang={lang}&timezone={timezone}'

    return Session().get(search).json()


def process(conn, json):
    cur = conn.cursor()
    now = datetime.datetime.now()
    for record in json['records']:
        insert = "INSERT INTO data (nom, places_restantes, derniere_mise_a_jour_base, now) " \
                 "VALUES ('{nom}', {places_restantes}, '{derniere_mise_a_jour_base}', '{now}')" \
            .replace('{nom}', record['fields']['nom'])\
            .replace('{places_restantes}', str(record['fields']['places_restantes']))\
            .replace('{derniere_mise_a_jour_base}', record['fields']['derniere_mise_a_jour_base'])\
            .replace('{now}', str(now))
        cur.execute(insert)
    conn.commit()
    conn.close()


connection = create_server_connection()
if connection:
    process(connection, get_data())
