import datetime,glob

def check_robot():
    date = datetime.datetime.today().strftime("%Y_%m_%d")
    path =r'\\miacshare\COVID-списки\Входящие списки\для загрузки\Robot' +'\\'+ date + '\\' +'*'
    spisok = 'В директории Robot сейчас лежат файлы:'
    for file in glob.glob(path):
        spisok += '\n' + file.split('\\')[-1]
    return spisok

