import datetime,glob,os

def check_robot():
    date = datetime.datetime.today().strftime("%Y_%m_%d")
    path =os.getenv('path_robot') +'\\'+ date + '\\' +'*'
    spisok = 'В директории Robot сейчас лежат файлы:'
    for file in glob.glob(path):
        spisok += '\n' + file.split('\\')[-1]
    return spisok

