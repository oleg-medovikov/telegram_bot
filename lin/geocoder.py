import pandas as pd
import requests,os,sqlalchemy

from sending import send
from loader import get_dir

server  = os.getenv('server')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser') # Тут правильный двойной слеш!
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db')

eng = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}",pool_pre_ping=True)
con = eng.connect()


def get_coordinat(adr:str,TOKEN:str):
    url = f"https://geocode-maps.yandex.ru/1.x?format=json&lang=ru_RU&kind=house&geocode={adr}&apikey={TOKEN}&bbox=29.5,60.25~31,59.5&rspn=1"
    try:
        data = requests.get(url).json()
    except:
        return None
    try:
        position = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']
    except:
        return None 
    else:
        return position.split(' ')[1] +' '+ position.split(' ')[0]

def update_table(index,cord):
    sql= f""" update  [COVID].[robo].[UMSRS_coordinates]
                set coordinats = '{cord}'
                where [idUMSRS] = {index}
    """
    try:
        con.execute(sql)
    except:
        try:
            con.execute(sql)
        except:
            pass


def search_coordinats(a):
    TOKENS = open('/home/sshuser/jupyter/tmp/yandex_api.txt', 'r').read().split('\n')
    
    file = get_dir('path_robot') + '/прикрепления/zone+coordinats.xlsx' 
    #df = pd.read_excel(file)

    for TOKEN in TOKENS:
        #df = pd.read_sql("""select top(1000) * from  [dbo].[zone_and_coordinates] where  coordinats = 'не найдено'""", con)
        
        sql = """SELECT top(1000) [idUMSRS],[adress],[coordinats]
                    FROM [COVID].[robo].[UMSRS_coordinates]
                        where [adress] is not null and [coordinats] is null"""
        
        df = pd.read_sql(sql, con)
        send('admin', 'Использую токен под номером ' + str(TOKENS.index(TOKEN) + 1) )
        for i in df.index:
            try:
                ADDRESS = df.at[i,'adress'].replace(' г','')
                #ADDRESS =  str(df.at[i,'TOWN_NAME'].replace('г.',''))+'+'+str(df.at[i,'GEONIM_NAME']) +"+"+ str(df.at[i,'HOUSE']) +'+'+ str(df.at[i,'KORPUS']) +'+'+ str(df.at[i,'TOWN_AREA_NAME'])
                #ADDRESS = df.at[i,'TOWN_NAME'] +"+"+df.at[i,'TOWN_AREA_NAME'] + "+" +df.at[i,'GEONIM_NAME'] + "+" +df.at[i,'GEONIM_TYPE_NAME'] + "+" +df.at[i,'HOUSE'] +'+'+ df.at[i,'KORPUS']
            except:
                continue
            else:
                ADDRESS = ADDRESS.replace(' ','+').replace('++','')
                coordinats = get_coordinat(ADDRESS,TOKEN)
                if not coordinats is None:
                    update_table(int(df.at[i,'idUMSRS']), coordinats)
                    #df.loc[i,'coordinats'] = coordinats
                else:
                    update_table(int(df.at[i,'idUMSRS']), 'не найдено')
                    
    send('admin', 'Закончил искать адреса на сегодня.' )
    return 1


