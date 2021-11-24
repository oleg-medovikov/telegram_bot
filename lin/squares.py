import numpy as np
import pandas as pd
from numpy import pi,sin,cos,sqrt,arctan
from PIL import Image
from PIL import Image
from PIL import Image, ImageDraw, ImageFont
import datetime,os,locale,sqlalchemy

locale.setlocale(locale.LC_ALL, "ru_RU.UTF-8")

from loader import get_dir

server  = os.getenv('server')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser') # Тут правильный двойной слеш!
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db')

eng = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}",pool_pre_ping=True)
con = eng.connect()


def paint_otchet_vachin(a):
    def add_point(x,y,x0y0,color):
        array[ x0y0[0] + int(y) , x0y0[1] + int(x) ] = color

    def paint_square(value,border,x0y0,color,flag):
        sum = 0  
        if flag:
            for i in range(value):
                for k in range(value):
                    add_point(i, x_max-1 - k,x0y0,color)
                    
        else:
            for i in range(value):
                for k in range(value):
                    if x_max-1 - i > border:
                        add_point(x_max-1 - i,x_max-1 - k,x0y0,color) 
                    else:
                        sum+=1            

        if sum:
            height = int(sum/ (x_max-border))
            for i in range(x_max - border):
                for k in range(2*height):
                    add_point(x_max-1 - i, border-k,x0y0, color)            

    def paint_line(value,width):
        for i in range(x_max):
            for k in range(width):
                add_point(value - k,x_max-1 - i,x0y0,(0,0,0))
            
    def calculate_a(value):
        return int(np.sqrt(value)) 
        
        

    resolution = (3508*2,2480*2)

    #color_healthy = (152,251,152)
    color_healthy = (34,139,34)
    color_ill     = (255,211,0)
    color_death   = (178,34,34)
    
    sql = """SELECT [VACHIN],[VACHIN_SICK],[VACHIN_DEATH],[NOVACHIN],[NOVACHIN_SICK],[NOVACHIN_DEATH]
                FROM [COVID].[dbo].[v_DrawingSquares]"""

    df = pd.read_sql(sql,con)

    POPULATION = 4421080

    VACHIN          = int(df.at[0,'VACHIN'])
    NOVACHIN        = int(df.at[0,'NOVACHIN'])
    VACHIN_SICK     = int(df.at[0,'VACHIN_SICK'])
    NOVACHIN_SICK   = int(df.at[0,'NOVACHIN_SICK'])
    VACHIN_DEATH    = int(df.at[0,'VACHIN_DEATH'])
    NOVACHIN_DEATH  = int(df.at[0,'NOVACHIN_DEATH'])

    x_max = calculate_a(POPULATION)
    array = np.zeros([resolution[0],resolution[1],3],dtype=np.uint8)
    array.fill(255)

    # рисунок вакцинированные
    x0y0 = (int(0.05*resolution[0]),
            int(0.05*resolution[1]) )
    line = x_max


    paint_square(calculate_a(VACHIN),  line, x0y0, color_healthy, True)
    paint_square(calculate_a(VACHIN_SICK), line, x0y0, color_ill, True)
    paint_square(calculate_a(VACHIN_DEATH),line,x0y0,color_death, True)



    # рисунок невакцинированные

    x0y0 = (int(0.1*resolution[0]) + calculate_a(VACHIN),
            int(0.05*resolution[1]) )
    line = x_max


    paint_square(calculate_a(NOVACHIN),  line, x0y0, color_healthy, True)
    paint_square(calculate_a(NOVACHIN_SICK), line, x0y0, color_ill, True)
    paint_square(calculate_a(NOVACHIN_DEATH),line,x0y0,color_death, True)


    # Общий рисунок 
    #line = int(0.5*x_max*VACHIN/NOVACHIN)
    #x0y0 = (int(0.65*resolution[0]),int(0.05*resolution[1]) )

    #paint_square(calculate_a(POPULATION),  line, x0y0, color_healthy, True)
    #paint_square(calculate_a(VACHIN_SICK), line, x0y0, color_ill,     True)
    #paint_square(calculate_a(VACHIN_DEATH),line,x0y0,color_death, True)


    #paint_square(calculate_a(NOVACHIN_SICK),line,x0y0,color_ill, False)
    #paint_square(calculate_a(NOVACHIN_DEATH),line,x0y0,color_death, False)
    #paint_line(line,5)
    
    img = Image.fromarray(array)

    file = get_dir('temp') + '/otchet.png'
    img.save(file)

    image = Image.open(file)
    idraw = ImageDraw.Draw(image)
    
    ttf = get_dir('help') + '/DejaVuSans.ttf'
    # Заголовок
    font = ImageFont.truetype(ttf, size=80 )

    text_header = u"Отчет о соотношении вакцинированных и невакцинированных больных\n\nв городе Санкт-Петербурге\n\nна " + datetime.datetime.now().strftime('%d.%m.%Y')
    x0y0 = (int(0.1*resolution[0]),
            int(0.05*resolution[1]) )
    idraw.text(x0y0, text_header, font=font, fill = (0,0,0))

    # Вакцинированные

    font = ImageFont.truetype(ttf, size=75)

    text_vachin = u"Вакцинированных на данный момент\n\nВсего: " +format(VACHIN,'n') + ' ('+ format(round(100*VACHIN/POPULATION, 2 ), 'n')+ '%) человек.'\
                    + u"\n\nИз них заболело: " + format(VACHIN_SICK, 'n') +" (" +  format(round(100*VACHIN_SICK/VACHIN, 2 ), 'n') +'%)' \
                    + u"\n\nИз них умерло: " + format(VACHIN_DEATH, 'n') + " (" +format(round(100*VACHIN_DEATH/VACHIN, 2 ),'n') +'%)'    


    x0y0 = (int(0.36*resolution[0]),
            int(0.25*resolution[1]) )
    idraw.text(x0y0, text_vachin, font=font, fill = (0,0,0))

    # Невакцинированные

    font = ImageFont.truetype(ttf, size=75)

    text_novachin = u"Невакцинированные на данный момент\n\nВсего: " +format(NOVACHIN,'n') +' ('+ format(round(100*NOVACHIN/POPULATION, 2 ), 'n')+ '%) человек.'\
                    + u"\n\nИз них заболело: " + format(NOVACHIN_SICK, 'n') + " (" +  format(round(100*NOVACHIN_SICK/NOVACHIN, 2 ), 'n') +'%)' \
                    + u"\n\nИз них умерло: " + format(NOVACHIN_DEATH, 'n') + " (" +format(round(100*NOVACHIN_DEATH/NOVACHIN, 2 ), 'n') +'%)'    


    x0y0 = (int(0.36*resolution[0]),
            int(0.65*resolution[1]) )
    idraw.text(x0y0, text_novachin, font=font, fill = (0,0,0))

    # Общий

    #font = ImageFont.truetype(ttf, size=75)

    #text_all = u"Сравнение с населением Санкт-Петербурга\n\nНаселение берется: " +format(POPULATION,'n') + ' человек.'\
    #                + u"\n\nВакцинированных: " + format(VACHIN, 'n') + ",\n\n    что составляет " +  format(round(100*VACHIN/POPULATION, 2 ),'n') +'%' \
    #                + u"\n\nНевакцинированных: " + format(NOVACHIN, 'n') + ",\n\n    что составляет " +  format(round(100*NOVACHIN/POPULATION, 2 ),'n') +'%' \
    #                + u"\n\nВакцинированных больных: " + format(VACHIN_SICK, 'n') + ",\n\n    что составляет " +  format(round(100*VACHIN_SICK/POPULATION, 2 ),'n') +'%' \
    #                + u"\n\nНевакцинированных больных: " + format(NOVACHIN_SICK, 'n') + ",\n\n    что составляет " + format(round(100*NOVACHIN_SICK/POPULATION, 2 ),'n') +'%'\
    #                + u"\n\nВакцинированных умерших: " + format(VACHIN_DEATH, 'n') + ",\n\n    что составляет " + str(round(100*VACHIN_DEATH/POPULATION, 2 )) +'%' \
    #                + u"\n\nНевакцинированных умерших: " + format(NOVACHIN_DEATH, 'n') + ",\n\n    что составляет " + str(round(100*NOVACHIN_DEATH/POPULATION, 2 )) +'%' 

    #x0y0 = (int(0.36*resolution[0]),
    #        int(0.93*resolution[1]) )
    #idraw.text(x0y0, text_all, font=font, fill = (0,0,0))

    image.save(file)

    return file
