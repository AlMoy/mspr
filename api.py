from requests import Session


def poitier():
    lang = 'fr'
    timezone = 'Europe%2FParis'
    base_url = '{base_url}?dataset={dataset}&q=&lang={lang}&timezone={timezone}' \
        .replace('{base_url}', 'https://data.grandpoitiers.fr/api/records/1.0/search/') \
        .replace('{lang}', lang) \
        .replace('{timezone}', timezone)

    real_time_dataset = 'mobilites-stationnement-des-parkings-en-temps-reel'
    data_metier_dataset = 'mobilite-parkings-grand-poitiers-donnees-metiers'

    real_time_url = base_url.replace('{dataset}', real_time_dataset)
    data_metier_url = base_url.replace('{dataset}', data_metier_dataset)

    real_time_json = Session().get(real_time_url).json()
    data_metier_json = Session().get(data_metier_url).json()

    return real_time_json, data_metier_json


def cleaning_poitier(json):
    real_time_json, data_metier_json = json
    json = []

    for record in real_time_json['records']:
        data = {}
        for key, value in record['fields'].items():
            if key != 'id':
                data[key] = value
        json.append(data)

    for record in data_metier_json['records']:
        data = {}
        for key, value in record['fields'].items():
            if key != 'id':
                data[key] = value
        json.append(data)

    return json
