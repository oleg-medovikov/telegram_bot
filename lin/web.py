import requests
import pandas as pd
import folium
import os
from folium.plugins import MarkerCluster

from loader import get_dir

NO_WORD = [
    'бокс', 'диспансеризация', 'углубленная', 'тест',
    'выписки', 'мазки', 'профилактики', 'дежурный',
    'профосмотр', 'кроме', 'фильтр', 'телемедицина',
    'неотложной', 'после', 'офтальмолог', 'перенесших',
    'диагн', 'первичный', 'переболевших', 'направление'
        ]


class my_except(Exception):
     pass


def vacine_talon(a):
    url = os.getenv('url845').replace('845', '852')
    req = requests.get(url, verify=False)
    try:
        data = req.json()
    except:
        raise my_except(req.text[:50])

    try:
        df = pd.DataFrame(data=data)
    except Exception as e:
        raise my_except(str(e))

    df = df.drop_duplicates()

    df = df.loc[~df['организация'].str.contains('|'.join(NO_WORD))]

    df.index = range(len(df))

    if not len(df):
        raise my_except('Нет данных от нетрики')

    mo = pd.read_json(get_dir('help') + '/mo64.json')
    log = ''
    for i in range(len(df)):
        key = df.at[i, 'moLev2_key']
        try:
            df.loc[i, 'Краткое стандартизованное наименование'] = mo.loc[mo['Код'] == key, 'Краткое стандартизованное наименование'].unique()[0]
        except:
            log += '\nне удалось найти название для: ' + key
        try:
            df.loc[i, 'Долгота'] = mo.loc[mo['Код'] == key, 'Долгота'].unique()[0]
            df.loc[i, 'Широта'] = mo.loc[mo['Код'] == key, 'Широта'].unique()[0]
        except:
            log += '\nне удалось получить координаты для: ' + key
        try:
            orgid =  mo.loc[mo['Код'] == key, 'orgid' ].unique()[0]
            df.loc[i,'организация'] = mo.loc[mo['Код'] == orgid, 'Краткое стандартизованное наименование' ].unique()[0]
        except:
            log += '\nне удалось найти название организации: ' + key

        try:
            df.loc[i,'link']  = mo.loc[mo['Код'] == key, 'link' ].unique()[0]
        except:
            log += '\nне удалось найти ссылку для:' + key

    if len(log):
       pass# send('',log)

    lat = pd.to_numeric(df['Широта'])
    lon = pd.to_numeric(df['Долгота'])
    elevation = pd.to_numeric(df['cntAppointments'])
    name = df['Краткое стандартизованное наименование']
    org = df['организация']
    cab = df['doctorFio']
    ndate = df['cntAvailableDates']
    date = df['Ближайший доступный талон']
    link = df['link']


    def color_change(elev):
        if(elev > 100):
            return('green')
        elif(50 <= elev <100):
            return('orange')
        else:
            return('red')

    m = folium.Map(name= 'test',location=[59.95020, 30.31543],
                   zoom_start =11, max_zoom=14,min_zoom=10,
                   min_lat=59.5, max_lat=60.5, min_lon=29.5, max_lon=31, max_bounds=True,
                   prefer_canvas=True,
                   #tiles = "CartoDB positron",
                   #tiles = "CartoDB dark_matter",
                  )
    lgd_txt = '<span style="color: {col};">{txt}</span>'
    marker_cluster = MarkerCluster().add_to(m)
    for lat, lon, elevation, org, name, cab, ndate, date,link in zip(lat, lon, elevation, org, name, cab, ndate, date, link):
        html = """<style>
              .table {
              font-family: Helvetica, Arial, sans-serif;
                width: 300px;
              }
              .table__heading, .table__cell {
                padding: .5rem 0;
              }
              .table__heading {
                text-align: left;
                font-size: 1.5rem;
                border-bottom: 1px solid #ccc;
              }
              .table__cell {
                line-height: 2rem;
              }
              .table__cell--highlighted {
                font-size: 1.25rem;
                font-weight: 300;
              }
              .table__button {
                display: inline-block;
                padding: .75rem 2rem;
                margin-top: 1rem;
                font-weight: 300;
                text-decoration: none;
                text-transform: uppercase;
                color: #fff;
                background-color: #f82;
                border-radius: .10rem;
              }
              .table__button:hover {
                background-color: #e62;
              }""" + f"""
            </style>
            <table cellpadding="1" cellspacing="1"  bordercolor="white" border="1" class="table">
              <thead>
                <tr>
                  <th colspan="2" scope="col" class="table__heading">{org}</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td colspan="2" class="table__cell table__cell--highlighted">{name}</td>
                </tr>
                 <tr>
                  <td colspan="2" class="table__cell table__cell--highlighted">{cab}</td>
                </tr>
                <tr>
                  <td class="table__cell">Доступно талонов:</td>
                  <td class="table__cell table__cell--highlighted">{elevation}</td>
                </tr>
                <tr>
                  <td class="table__cell">Количество дней в ближайшие 2 месяца, на которые есть талоны:</td>
                  <td class="table__cell table__cell--highlighted">{ndate}</td>
                </tr>
                <tr>
                  <td class="table__cell">Ближайший доступный талон:</td>
                  <td class="table__cell table__cell--highlighted">{date}</td>
                </tr>
                <tr>
                  <td colspan="2" class="table__cell">
                    <a href={link} target="_blank" class="table__button">Записаться</a>
                  </td>
                </tr>
              </tbody>
            </table>
        """
        iframe = folium.Html(html,script=True)
        popup = folium.Popup(iframe)
        try:
            folium.CircleMarker(location=[lat, lon],
                                radius = 13, popup=popup, fill_color=color_change(elevation),
                                color="gray", fill_opacity = 0.8).add_to(marker_cluster)
        except:
            pass
            #send('','не удалось')

    file = get_dir('temp') + '/map_light.html'
    m.save(file)
    # добавляем скрипт обновления
    html = open(file,'r').read()

    html = html.rsplit('</script>',1)[0]
    
    html +="""
            setTimeout(function(){
            location.reload();
        }, 300000);



    </script>

    """
    with open(file, 'w') as f:
        f.write(html)

    command = f"/usr/bin/scp {file} vacmap@miacsitenew:/home/vacmap/vacmap/vacmap.html"
    os.system(command)
    return 1
