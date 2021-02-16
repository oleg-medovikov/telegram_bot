import pandas as pd
import datetime,warnings,shutil
warnings.filterwarnings('ignore')
from sqlalchemy import *

from loader import get_dir

class my_except(Exception):
    pass


def regiz_decomposition(a):
    # Создаем подключение
    con = create_engine('mssql+pymssql://miacbase3/PNK_NCRN',convert_unicode=True)
    answer = pd.read_sql('SELECT * FROM [dbo].[v_Answer_MO]', con)
    if len(answer) > 0 :
        ftp_user = answer.ftp_user.unique()
        statistic = pd.DataFrame()
        for ftp in ftp_user:
            otvet = answer[answer.ftp_user == ftp]
            lpu_name = otvet.LPU_name.unique()[0]
            del otvet['ftp_user']
            del otvet['LPU_level1_key']
            del otvet['LPU_name']
            #otvet.SnilsDoctor = otvet.SnilsDoctor.replace('00000000000', '')
            otvet.rename(columns = { 'HistoryNumber':'Номер истории болезни' 
                                                            , 'OpenDate':'Дата открытия СМО'
                                                            #, 'IsAmbulant':'Признак амбулаторного СМО'
                                                            #, 'SnilsDoctor':'СНИЛС врача'
                                                            , 'Error':'Ошибка, выявленная в РЕГИЗ'
                                                      }, inplace = True)
            path_otvet = str(get_dir('regiz')+'\\' + ftp +r'\Ответы'+ r'\_'+ str(datetime.datetime.now().date()) +' '+ lpu_name+'.xlsx').replace('"','')
            with pd.ExcelWriter(path_otvet) as writer:
                try:
                    otvet.to_excel(writer,sheet_name='номера',index=False)
                except PermissionError:
                    pass

            k = len(statistic)
            statistic.loc[k,'MOName'] = lpu_name
            statistic.loc[k,'NameFile'] = path_otvet.split('\\')[-1]
            statistic.loc[k,'CountRows'] = len(otvet)
            statistic.loc[k,'TextError'] = ''
            statistic.loc[k,'OtherFiles'] = ''
            statistic.loc[k,'DateLoadFile'] = datetime.datetime.now()
            statistic.loc[k,'InOrOut'] = 'Out'

        path_log = get_dir('regiz_svod')+'\\'+ str(datetime.datetime.now().date()) + r' лог разложения.xlsx'

        with pd.ExcelWriter(path_log) as writer:
            statistic.to_excel(writer,sheet_name='логи',index=False)
        temp_log = get_dir('temp') + '\\' + path_log.split('\\')[-1]
        shutil.copyfile(path_log,temp_log)
        statistic.to_sql(
                    'JrnLoadFiles',
                    con,
                    schema='logs',
                    if_exists='append',
                    index=False
            )
        print('Загружены логи')

        # Очистка таблиц в базе данных
        from sqlalchemy.orm import sessionmaker

        Session = sessionmaker(bind=con)
        session = Session()
        session.execute('''
        -- после отправки в МО
        -- Обновление даты отправки ответа в МО
        UPDATE [dbo].[HistoryAnswerFromShowcase]
          SET [DateAnswerMO] = GETDATE()
          WHERE [DateAnswerMO] is null 

        -- Обновление даты отправки ответа в МО
        UPDATE [dbo].[ErrorRequest]
          SET [DateSend] = GETDATE()
          WHERE [DateSend] is null
          AND [TextError] not like 'MIAC:%'
        ''')
        session.commit()
        session.close()
        
        
        print('Региз разложен по папкам')
        with open(r'\\MIAC-ftp-emts\FTP\ORI\REGIZ\свод\log.txt', 'a') as file:
            file.write("  Региз разложен по папкам " + str(datetime.datetime.now()) + '\n' )
            return temp_log
    else:
        with open(r'\\MIAC-ftp-emts\FTP\ORI\REGIZ\свод\log.txt', 'a') as file:
            file.write("  РЕГИЗ Нечего раскладывать по папкам " + str(datetime.datetime.now()) + '\n' )
        raise my_except("Нечего расскладывать по папкам!")
