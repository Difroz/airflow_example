import requests
import pandas as pd
import logging

URL = 'http://www.cbr.ru/scripts/XML_daily.asp'
logging.basicConfig(level=logging.INFO)

def get_cur_data(url, path):
    logging.info('Loading data from site...')
    r = requests.get(url)  # Делаю запрос к сайту
    content = r.content  # Получаю содержание ответа
    logging.info("New data loaded")
    logging.info('Saving new data')
    df = pd.read_xml(content, encoding='cp1251')  # Преобразую полученный xml в dataframe
    df.to_csv(path, index=False)
    logging.info('New data Saved.')


def change_data(path_in, path_out):
    df = pd.read_csv(path_in)
    df['Value'] = df['Value'].str.replace(',', '.').astype('float64')
    res = df.loc[df['Value'] <= 50]
    res.to_csv(path_out, index=False)