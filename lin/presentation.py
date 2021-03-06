import os,datetime,sqlalchemy
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.util import Pt
from pptx.enum.dml import MSO_THEME_COLOR
from pptx.enum.dml import MSO_THEME_COLOR
from pptx.util import Inches
import pandas as pd
from loader import get_dir

server  = os.getenv('server')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser') # Тут правильный двойной слеш!
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db')

eng = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}",pool_pre_ping=True)
con = eng.connect()

valume = [
     ['2 группа замечаний: Нет сведений ОМС (Стац.)','Нет сведений ОМС','Стационарная']
    ,['2 группа замечаний: Нет сведений ОМС (Амб.)','Нет сведений ОМС','Амбулаторная']
    #,['4 группа замечаний: Без амбулаторного этапа (Стац.)','Нет амбулаторного этапа','Стационарная']
    ,['4 группа замечаний: Без амбулаторного этапа (Амб.)','Нет амбулаторного этапа','Амбулаторная']
    ,['5 группа замечаний: Без исхода заболевания больше 45 дней (Стац.)','Без исхода заболевания больше 45 дней','Стационарная']
    ,['5 группа замечаний: Без исхода заболевания больше 45 дней (Амб.)','Без исхода заболевания больше 45 дней','Амбулаторная']
    ,['8 группа замечаний: Неправильный тип лечения (Стац.)', 'Неверный вид лечения', 'Стационарная']
    ,['8 группа замечаний: Неправильный тип лечения (Амб.)', 'Неверный вид лечения', 'Амбулаторная']
    ,['11 группа замечаний: Количество дублированных  УНРЗ в одном МО (Стац.)', 'Количество дублей','Стационарная']
    ,['11 группа замечаний: Количество дублированных  УНРЗ в одном МО (Амб.)', 'Количество дублей','Амбулаторная']
    ,['12 группа замечаний: Нет ПАД (Стац.)','Нет ПАД','Стационарная' ]
    ,['12 группа замечаний: Нет ПАД (Амб.)','Нет ПАД','Амбулаторная' ]
    ,['13 группа замечаний: Нет дневниковых записей','Нет дневниковых записей','Стационарная']
    ,['13 группа замечаний: Нет дневниковых записей','Нет дневниковых записей','Амбулаторная']
]



def generate_pptx(date):
    def create_slide(title,type_error,type_org,date_start):
        sql = f"""
        select distinct d1.[Медицинская организация] ,d1.eror1  , d2.eror2 
                    , d1.eror1 -  d2.eror2 as 'Динамика'           
            from (
        SELECT [Медицинская организация]
              , isnull([{type_error}],0) as 'eror1'
          FROM [COVID].[robo].[cv_Zamechania_fr]
          where [дата отчета] = '{date_start_sql}' 
            and [Тип организации] = '{type_org}'  ) as d1
          full join (
        SELECT [Медицинская организация]
              , isnull([{type_error}],0) as 'eror2'
          FROM [COVID].[robo].[cv_Zamechania_fr]
          where [дата отчета] = cast(getdate() as date)
          and [Тип организации] = '{type_org}' ) as d2
          on (d1.[Медицинская организация] = d2.[Медицинская организация])
          order by  d1.eror1 - d2.eror2  DESC , d1.eror1 DESC
        """
        frame = pd.read_sql(sql,con)
        frame.fillna(0,inplace=True)
        frame ['Динамика'] = pd.to_numeric(frame['Динамика'])
        df = frame.loc[~frame['Динамика'].isin([0])]
        if len(df) > 16:
            df = df.head(8).append(df.tail(8))
            df.index = range(len(df))
        else:
            df.index = range(len(df))
        sum_err1 = frame['eror1'].sum()
        sum_err2 = frame['eror2'].sum()
        title_only_slide_layout = prs.slide_layouts[5]
        slide = prs.slides.add_slide(title_only_slide_layout)
        shapes = slide.shapes

        shapes.title.text = title
        shapes.title.text_frame.paragraphs[0].font.color.rgb = RGBColor(255,0, 0)
        shapes.title.text_frame.paragraphs[0].font.size = Pt(30)

        body_shape = shapes.placeholders[0]
        tf = body_shape.text_frame
        p = tf.add_paragraph()
        p.text = 'Общее число замечаний на дату ' + date_start + ' было ' + str(sum_err1).replace('.0','')
        p.font.color.rgb = RGBColor(20,20,20)
        p.font.size = Pt(18)
        
        p = tf.add_paragraph()
        p.text = 'А на дату ' + date_end + ' cтало ' + str(sum_err2).replace('.0','')
        p.font.color.rgb = RGBColor(20,20,20)
        p.font.size = Pt(18)
        
        p = tf.add_paragraph()
        p.text = 'Общая динамика: ' + str(sum_err2 - sum_err1).replace('.0','')
        p.font.color.rgb = RGBColor(20,20,20)
        p.font.size = Pt(18)
        
        rows = len(df) + 1
        cols = 5
        left = Inches(0.5)
        top = Inches(2)
        width = Inches(5)
        height = Inches(0.1)

        shape = shapes.add_table(rows, cols, left, top, width, height)
        table = shape.table
        tbl = shape._element.graphic.graphicData.tbl
        style_id = '{9DCAF9ED-07DC-4A11-8D7F-57B35C25682E}'
        tbl[0][-1].text = style_id

        # set column widths
        table.columns[0].width = Inches(0.5)
        table.columns[1].width = Inches(4.0)
        table.columns[2].width = Inches(1.5)
        table.columns[3].width = Inches(1.5)
        table.columns[4].width = Inches(1.5)
        # write column headings
        table.cell(0, 0).text = '№'
        table.cell(0, 1).text = 'Медицинская организация'
        table.cell(0, 2).text = date_start
        table.cell(0, 3).text = date_end
        table.cell(0, 4).text = 'Динамика'
        for i in range(len(df)):
            table.cell(i+1, 0).text = str(i+1) 
            table.cell(i+1, 0).text_frame.paragraphs[0].font.size = Pt(12)
            table.cell(i+1, 1).text = df.at[i,'Медицинская организация']
            table.cell(i+1, 1).text_frame.paragraphs[0].font.size = Pt(10) 
            table.cell(i+1, 2).text = str(df.at[i,'eror1']).replace('.0','')
            table.cell(i+1, 2).text_frame.paragraphs[0].font.size = Pt(12)
            table.cell(i+1, 3).text = str(df.at[i,'eror2']).replace('.0','')
            table.cell(i+1, 3).text_frame.paragraphs[0].font.size = Pt(12)
            table.cell(i+1, 4).text = str(df.at[i,'Динамика']).replace('.0','')
            table.cell(i+1, 4).text_frame.paragraphs[0].font.size = Pt(12)

    date_start_sql = date
    date_start = pd.to_datetime(date).strftime("%d.%m.%Y")
    date_end = datetime.datetime.today().strftime("%d.%m.%Y")
    prs = Presentation()
    title_slide_layout = prs.slide_layouts[0]
    slide = prs.slides.add_slide(title_slide_layout)
    title = slide.shapes.title
    subtitle = slide.placeholders[1]
    # Первый слайд
    title.text = "Замечания по ведению Федерального регистра лиц, больных COVID-19"
    subtitle.text = "По состоянию на " + datetime.datetime.today().strftime("%d.%m.%Y")

    # Второй слайд


    bullet_slide_layout = prs.slide_layouts[5]
    slide = prs.slides.add_slide(bullet_slide_layout)
    shapes = slide.shapes

    title_shape = shapes.title
    body_shape = shapes.placeholders[0]

    title_shape.text = 'Группы замечаний по ведению Регистра на '+ datetime.datetime.today().strftime("%d.%m.%Y")
    tf = body_shape.text_frame
    date_in_text = (datetime.datetime.now()-datetime.timedelta(45)).strftime("%d.%m.%Y")

    text = f"""Срок создания регистровой записи (УНРЗ) не соответствует дате установки диагноза – более 7 дней между датами (МЗ оценивает регион) (Количество регистровых записей, внесенных с задержкой больше недели, за последний месяц)
    Отсутствие информации о номере Полиса ОМС в разделе «Медицинское страхование»
    Не заполнен Раздел «Результаты ежедневного наблюдения» (дневниковые записи) – по данным МЗ РФ
    Поле «исход заболевания» заполнено «переведен в другую МО» без открытия следующего этапа лечения («зависшие» пациенты более 7 дней от даты перевода в др. МО)
    Поле «исход заболевания» не заполнено с датой установки диагноза ранее {date_in_text} (длительность болезни более 45 дней)
    Количество УНРЗ с заполненным полем «исход заболевания» «выздоровление» не соответствует оперативной отчетности поликлиник
    Количество УНРЗ с пустым полем «исход заболевания» (находящиеся под медицинским наблюдением в амбулаторных условиях) не соответствует оперативной отчетности поликлиник
    Поле «Вид лечения» ошибочно заполнено «стационарный» -  у пациентов, находящихся под медицинским наблюдением в амбулаторных условиях 
    Количество УНРЗ с заполненным Разделом «Результаты ежедневного наблюдения» в части поля «ИВЛ» не соответствует оперативной отчетности стационаров
    Количество УНРЗ пациентов, находящихся под медицинским наблюдением в стационарных условиях, не соответствует оперативной отчетности стационаров
    Дублированные  УНРЗ в одном МО
    Отсутствие прикрепленного файла патологоанатомического заключения"""

    rows = 12
    cols = 2 
    left = Inches(0.5)
    top = Inches(2)
    width = Inches(8)
    height = Inches(0.1)

    shape = shapes.add_table(rows, cols, left, top, width, height)

    tbl = shape._element.graphic.graphicData.tbl
    table = shape.table

    style_id = '{2D5ABB26-0587-4C30-8999-92F81FD0307C}'
    tbl[0][-1].text = style_id

    table.columns[0].width = Inches(0.5)
    table.columns[1].width = Inches(8.0)

    i = 0
    for t in text.split('\n'):
        table.cell(i, 0).text = str(i+1) + ')' 
        table.cell(i, 0).text_frame.paragraphs[0].font.size = Pt(12)
        table.cell(i, 1).text = t
        table.cell(i, 1).text_frame.paragraphs[0].font.size = Pt(11)
        i+=1
        
    # Третий слайд

    bullet_slide_layout = prs.slide_layouts[2]
    slide = prs.slides.add_slide(bullet_slide_layout)
    shapes = slide.shapes


    title_shape = shapes.title
    title_shape.text ='1 группа замечаний'
    title_shape.text_frame.paragraphs[0].font.color.rgb = RGBColor(255,0, 0)

    body_shape = shapes.placeholders[1]
    tf = body_shape.text_frame


    #df = pd.read_sql("Select count(*) from cv_fedreg where DATEDIFF(day,[Дата создания РЗ],getdate() ) < 31", con)
    df = pd.read_sql("""
	    select count(*)
	    from cv_fedreg 
	    where 
	    DATEDIFF(day,[Дата создания РЗ],getdate() ) < 31
	    and (DATEDIFF(day,[Дата создания РЗ],[Диагноз установлен] ) > 7 or DATEDIFF(day,[Дата создания РЗ],[Диагноз установлен] ) < -7 )
            """, con)


    p = tf.add_paragraph()
    p.text = 'за последние 30 дней: + ' + str(df.iat[0,0]) + ' УНРЗ'
    p.font.color.rgb = RGBColor(255,0,0)
    p.font.size = Pt(30)

    df = pd.read_sql("""
	    select count(*)
	    from cv_fedreg 
	    where 
	    (DATEDIFF(day,[Дата создания РЗ],[Диагноз установлен] ) > 7 or DATEDIFF(day,[Дата создания РЗ],[Диагноз установлен] ) < -7 )
            """, con)

    p = tf.add_paragraph()
    p.text = 'за всё время: ' + str(df.iat[0,0]) + ' УНРЗ'
    p.font.color.rgb = RGBColor(255,0,0)
    p.font.size = Pt(30)

    p = tf.add_paragraph()
    p = tf.add_paragraph()
    p.text = 'Срок создания регистровой записи (УНРЗ) не соответствует дате установки диагноза: разница более 7 дней после даты установки диагноза, количество взято за последний месяц'
    p.font.size = Pt(20)

    for title,type_error,type_org in valume:
        create_slide(title,type_error,type_org,date_start)

    pptx_file = get_dir('temp') + '/dynamic.pptx' 
    prs.save(pptx_file)

    return pptx_file
