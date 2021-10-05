import pandas as pd
import requests

from sending import send
from loader import get_dir

def get_coordinat(adr:str,TOKEN:str):
    url = f"https://geocode-maps.yandex.ru/1.x?format=json&lang=ru_RU&kind=house&geocode={adr}&apikey={TOKEN}&bbox=29.5,60.25~31,59.5&rspn=1"
    data = requests.get(url).json()
    try:
        position = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']
    except:
        return None 
    else:
        return position.split(' ')[1] +' '+ position.split(' ')[0]


def search_coordinats(a):
    TOKENS = open('/home/sshuser/jupyter/tmp/yandex_api.txt', 'r').read().split('\n')
    
    file = get_dir('path_robot') + '/прикрепления/zone+coordinats.xlsx' 
    df = pd.read_excel(file)
    start_number = len(df.loc[df['coordinats'].isnull()])

    for TOKEN in TOKENS:
        send('admin', 'Использую токен под номером ' + str(TOKENS.index(TOKEN) + 1) )
        for i in df.loc[(df['coordinats'].isnull()) & (df['TOWN_AREA_NAME']=='Курортный')].head(300).index:
            try:
                ADDRESS =  str(df.at[i,'GEONIM_NAME']) +"+"+ str(df.at[i,'HOUSE']) +'+'+ str(df.at[i,'KORPUS']) +'+'+ str(df.at[i,'TOWN_AREA_NAME'])
                #ADDRESS = df.at[i,'TOWN_NAME'] +"+"+df.at[i,'TOWN_AREA_NAME'] + "+" +df.at[i,'GEONIM_NAME'] + "+" +df.at[i,'GEONIM_TYPE_NAME'] + "+" +df.at[i,'HOUSE'] +'+'+ df.at[i,'KORPUS']
            except:
                continue
            else:
                ADDRESS = ADDRESS.replace(' ','+').replace('++','')
                coordinats = get_coordinat(ADDRESS,TOKEN)
                if not coordinats is None:
                    df.loc[i,'coordinats'] = coordinats

    df.to_excel(file,index=False)
    end_number = len(df.loc[df['coordinats'].isnull()])
    send('admin', 'Закончил искать адреса на сегодня. Всего найдено новых координат: ' + str( start_number - end_number ) )
    return 1


