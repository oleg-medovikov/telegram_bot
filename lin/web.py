import requests
import pandas as pd
import folium,os
from folium.plugins import MarkerCluster

from sending import send
from loader import get_dir

def vacine_talon(a):
    url = os.getenv('url845')
    data = requests.get(url).json()
    df = pd.DataFrame.from_dict(data)
    mo = pd.read_excel('/mnt/COVID-списки/jupyter/талоны/map.xlsx')

    df = df.merge(mo, how = 'left', left_on=['moLev1','moLev2','doctorFio'],right_on=['org','org2','cab'])
    df.index = range(len(df))

    lat = pd.to_numeric(df['moLev2_geo_x'])
    lon = pd.to_numeric(df['moLev2_geo_y'])
    elevation = pd.to_numeric(df['cntAppointments'])
    name = df['MO_x']
    cab = df['cab']
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

    for lat, lon, elevation, name, cab,ndate, date,link in zip(lat, lon, elevation, name, cab,ndate, date,link):
        html = """<style>
                  .table {
                  font-family: Helvetica, Arial, sans-serif;
                    width: 500px;
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
                    font-weight: 700;
                  }
                  .table__button {
                    display: inline-block;
                    padding: .75rem 2rem;
                    margin-top: 1rem;
                    font-weight: 700;
                    text-decoration: none;
                    text-transform: uppercase;
                    color: #fff;
                    background-color: #f82;
                    border-radius: .5rem;
                  }
                  .table__button:hover {
                    background-color: #e60;
                  }""" + f"""
                </style>

                <table cellpadding="1" cellspacing="1" class="table">
                  <thead>
                    <tr>
                      <th colspan="2" scope="col" class="table__heading">{name}</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td colspan="2" class="table__cell table__cell--highlighted">{cab}</td>
                    </tr>
                    <tr>
                      <td class="table__cell">Доступно талонов:</td>
                      <td class="table__cell table__cell--highlighted">{elevation}</td>
                    </tr>
                    <tr>
                      <td class="table__cell">Количество дней, на которые есть талоны:</td>
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
                </table>"""
        iframe = folium.IFrame(html)
        popup = folium.Popup(iframe,min_width=580,max_width=800)
        try:
            fg = folium.FeatureGroup(name= lgd_txt.format( txt= name + ' Доступно талонов: ' + str(elevation), col= color_change(elevation)))
            ci = folium.CircleMarker(location=[lat, lon], radius =10, popup=popup, fill_color=color_change(elevation), color="gray", fill_opacity = 1)
            fg.add_child(ci)
            m.add_child(fg)
            #ci.add_to(marker_cluster)
            #folium.FeatureGroup(name= lgd_txt.format( txt= name, col= color_change(elevation))).add_to(ci)
        except:
            print(lat,lon)

    folium.map.LayerControl('topleft', collapsed= True).add_to(m)
    file = get_dir('temp') + '/map_light.html'
    m.save(file)

    command = f"/usr/bin/scp {file} vacmap@miacsitenew:/home/vacmap/vacmap/vacmap.html"
    os.system(command)
    return 1
