import pandas as pd
import sqlalchemy,os,glob,folium,os
from folium.plugins import MarkerCluster
from loader import get_dir
from sending import send

server  = os.getenv('server_parus')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser') # Тут правильный двойной слеш!
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db_ncrn')




def vacine_talon(a):
    with sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}",pool_pre_ping=True).connect() as con:
        df = pd.read_sql("""select distinct * from tmp.VacAvailability where [Отчетное время] = (select max([Отчетное время]) FROM [PNK_NCRN].[tmp].[VacAvailability])
                        and (moLev2_geo_x is not null and moLev2_geo_y is not null )""", con)
    df = df.drop_duplicates(subset=['moLev1', 'moLev2'], keep='last')

    mo = pd.read_excel('/mnt/COVID-списки/jupyter/талоны/map.xlsx')
    df = df.merge(mo, how = 'left', left_on='doctorFio',right_on='cab')
    df = df.drop_duplicates(subset=['moLev1', 'moLev2'], keep='last')
    lat = pd.to_numeric(df['moLev2_geo_x'])
    lon = pd.to_numeric(df['moLev2_geo_y'])
    elevation = pd.to_numeric(df['Количество доступных талонов'])
    name = df['MO']
    cab = df['cab']
    date = df['MIN(Ближайший доступный талон)']
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



    #marker_cluster = MarkerCluster().add_to(map)

    lgd_txt = '<span style="color: {col};">{txt}</span>'

    for lat, lon, elevation, name, cab, date,link in zip(lat, lon, elevation, name, cab, date,link):
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
                      <td class="table__cell">Ближайший талон:</td>
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
            pass    #print(lat,lon)

    folium.map.LayerControl('topleft', collapsed= True).add_to(m)
    file = get_dir('temp') + '/map_light.html'
    m.save(file)

    command = f"/usr/bin/scp {file} vacmap@miacsitenew:/home/vacmap/vacmap/vacmap.html"
    os.system(command)
    send('admin', 'карта обновлена на время: ' + str(df['Отчетное время'].max()))
    return 1
