import pandas as pd
import datetime,warnings,shutil,sqlalchemy,os,glob
warnings.filterwarnings('ignore')
from loader import get_dir
from sending import send,send_file
from sqlalchemy.orm import sessionmaker
from xlrd import XLRDError

server  = os.getenv('server_parus')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser') # Тут правильный двойной слеш!
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db_ncrn')

eng = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}",pool_pre_ping=True)
con = eng.connect()

class my_except(Exception):
    pass

def sql_execute(sql):
    Session = sessionmaker(bind=con)
    session = Session()
    session.execute(sql)
    session.commit()
    session.close()

def regiz_decomposition(a):
    # Создаем подключение
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
            otvet.rename(columns = { 'HistoryNumber':'Номер истории болезни' 
                                                            , 'OpenDate':'Дата открытия СМО'
                                                            #, 'IsAmbulant':'Признак амбулаторного СМО'
                                                            #, 'SnilsDoctor':'СНИЛС врача'
                                                            , 'Error':'Ошибка, выявленная в РЕГИЗ'
                                                      }, inplace = True)
            path_otvet = str(get_dir('regiz')+'/' + ftp +'/Ответы/_'+ str(datetime.datetime.now().date()) +' '+ lpu_name+'.xlsx').replace('"','')
            with pd.ExcelWriter(path_otvet) as writer:
                try:
                    otvet.to_excel(writer,sheet_name='номера',index=False)
                except PermissionError:
                    statistic = statistic.append({'MOName' : lpu_name,
                                          'NameFile' : path_otvet.split('/')[-1],
                                          'CountRows' : len(otvet),
                                          'TextError' : 'Не удалось положить файл из-за ошибки доступа',
                                          'OtherFiles' : '',
                                          'DateLoadFile' : datetime.datetime.now(),
                                          'InOrOut' : 'Out' }, ignore_index=True)
                else:
                    statistic = statistic.append({'MOName' : lpu_name,
                                          'NameFile' : path_otvet.split('/')[-1],
                                          'CountRows' : len(otvet),
                                          'TextError' : '',
                                          'OtherFiles' : '',
                                          'DateLoadFile' : datetime.datetime.now(),
                                          'InOrOut' : 'Out' }, ignore_index=True)


        path_log = get_dir('regiz_svod')+'/'+ datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') + '_лог_разложения.xlsx'

        with pd.ExcelWriter(path_log) as writer:
            statistic.to_excel(writer,sheet_name='логи',index=False)
        temp_log = get_dir('temp') + '/' + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') + '_decomposition_log.xlsx'
        shutil.copyfile(path_log,temp_log)
        statistic.to_sql(
                    'JrnLoadFiles',
                    con,
                    schema='logs',
                    if_exists='append',
                    index=False
            )
        send('info','Загружены логи')
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
        
        send('info','Региз разложен по папкам')
        send_file('info',temp_log)
        try:
            file = open(get_dir('regiz_svod') + '/log.txt', 'a', encoding='utf-8')
        except:
            send('info', 'Файл лога недоступен!' )
        else:
            with file:
                file.write("  Региз разложен по папкам " + str(datetime.datetime.today()) + '\n' )
        
        return temp_log
    else:
        try:
            file = open(get_dir('regiz_svod') + r'/log.txt', 'a', encoding='utf-8')
        except:
            send('info', 'Файл лога недоступен!' )
        else:
            with file:
                file.write("  РЕГИЗ Нечего раскладывать по папкам " + str(datetime.datetime.today()) + '\n' )
        
        raise my_except("Нечего раскладывать по папкам!")

# Функция проверки колонок таблицы
def check_table(path_to_excel, names):
    sum_colum = len(names)
    num_colum = 0
    try:
        file = pd.read_excel(path_to_excel, header=None, na_filter = False)
    except:
        send('', 'Файл не читается\n' + path_to_excel )
        return [1,'Файл не читается','',0]
    if len(file) < 2:
        return [1,'Файл пустой','',0]
    for head in range(len(file)):
        if num_colum != sum_colum:
            coll = file.loc[head].tolist()
            num_colum = 0
            collum = []
            error = 0
            error_text = '' 
            for name in names:
                k = 0
                f = 0
                for col in coll:
                    if str(col).replace(' ','') == name.replace(' ','') : 
                        collum.append(k)
                        num_colum += 1 
                        f = 1
                    k+=1
                if f == 0:
                    error = 1
                    error_text = error_text + ' Не найдена колонка ' + name + ';'
        else:
            break
    return error,error_text,collum, head-1

def regiz_load_to_base(a):
    def org_mis(ftp):
        sql = f"""SELECT LEFT([NameMIS], LEN([NameMIS]) - 1)
            FROM (
                SELECT mis.[NameMIS] + ', '
                    FROM [PNK_NCRN].[nsi].[MISMO] as mis
                    inner join [nsi].[Organization] as org
                    on (mis.[level1_key] = org.level1_key)
               where org.ftp_user = '{ftp}'
            FOR XML PATH ('')
          ) c ([NameMIS])"""
        try:
            mis = pd.read_sql(sql,con).iat[0,0]
        except:
            return 'Не удалось узнать МИС'
        else:
            return str(mis)

    sql_execute("""
                TRUNCATE TABLE dbo.TempTableFromMO
                TRUNCATE TABLE nsi.Organization
            """)
    mo = pd.read_excel(get_dir('regiz_svod') + '/mo_directory.xlsx')
    mo1 = mo.rename(columns = { 'ftp_user':'ftp_user'
                        , 'oid':'OID'
                        , 'level1_key':'level1_key'
                        , 'МО_краткое наименование':'MONameSpr64'
                        , 'MO':'MOName'
                        , 'MO_полное':'MONameFull'
                        , 'email':'Email'
                        , 'active':'IsActive'
                        , 'ИОГВ' : 'IOGV'
                        })
    
    mo1.to_sql('Organization',con,schema='nsi',if_exists='append',index=False)
    names = ['Номер истории болезни','Дата открытия СМО','Признак амбулаторного СМО','СНИЛС врача']
    names_new = ['HistoryNumber','OpenDate','IsAmbulant','SnilsDoctor']
    path = get_dir('regiz') + '/ori.regiz*/_Входящие/*.xls' 
    files = glob.glob(path) + glob.glob(path + 'x')
    list_ = []
    remove_files = []
    stat = pd.DataFrame()
    df = pd.DataFrame()

    for file in files:
        other_files = str(glob.glob(file.rsplit('/',1)[0] + '/*'))
        send('',file)       
        if len(mo.loc[mo['ftp_user'] == file.split('/')[5], 'MO']): 
            organization = mo.loc[mo['ftp_user'] == file.split('/')[5], 'MO'].values[0]
        else:
            organization = 'Не определена'

        try:
            df = pd.read_excel(file,usecols=names,dtype=str)
        except XLRDError: # если это html файл 
            try:
                df = pd.read_html(file)
            except ValueError: # если не удалось распарсить html
                pass
                #send('',file)
            else:
                df = pd.concat(df)
                df = df[names].applymap(str)
                df.columns=names_new
                stat = stat.append({'MOName'       : organization,
                                      'NameFile'     : file,
                                      'CountRows'    : len(df),
                                      'TextError'    : 'Файл HTMl удачно распарсен',
                                      'OtherFiles'   : other_files,
                                      'mis'          : org_mis(file.split('/')[5]),
                                      'DateLoadFile' : datetime.datetime.now(),
                                      'InOrOut'      : 'IN'}, ignore_index=True)
        except ValueError as exc: # Если не найдены колонки
            if str(exc) == r"File is not a recognized excel file":
                try:
                    df = pd.read_html(file)
                except ValueError: # если не удалось распарсить html
                    send('',file)
                else:
                    if len(df) > 0:
                        df = pd.concat(df)
                        df = df[names].applymap(str)
                        df.columns=names_new
                        stat = stat.append({'MOName'       : organization,
                                              'NameFile'     : file,
                                              'CountRows'    : len(df),
                                              'TextError'    : 'Файл HTMl удачно распарсен',
                                              'OtherFiles'   : other_files,
                                              'mis'          : org_mis(file.split('/')[5]),
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
                    else:
                        stat = stat.append({'MOName'       : organization,
                                              'NameFile'     : file,
                                              'CountRows'    : 0,
                                              'TextError'    : 'Файл HTMl не удалось распарсить',
                                              'OtherFiles'   : other_files,
                                              'mis'          : org_mis(file.split('/')[5]),
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
            else:
                try:
                    df = pd.read_excel(file, usecols=names_new,dtype=str)
                except:
                    if len(pd.read_excel(file).columns) == 4:
                        for i in range(10): # Ищем по строкам 
                            try:
                                df = pd.read_excel(file,usecols=names, skiprows =i,dtype=str)
                            except:
                                if i == 9:
                                    stat = stat.append({'MOName'       : organization,
                                                      'NameFile'     : file,
                                                      'CountRows'    : 0,
                                                      'TextError'    : 'Не найдена одна из колонок',
                                                      'OtherFiles'   : other_files,
                                                      'mis'          : org_mis(file.split('/')[5]),
                                                      'DateLoadFile' : datetime.datetime.now(),
                                                      'InOrOut'      : 'IN'}, ignore_index=True)
                                else:
                                    pass
                            else:
                                df.columns=names_new
                                stat = stat.append({'MOName'       : organization,
                                                      'NameFile'     : file,
                                                      'CountRows'    : len(df),
                                                      'TextError'    : 'Файл прочитан, но пришлось поискать шапку на строке номер ' + str(i),
                                                      'OtherFiles'   : other_files,
                                                      'mis'          : org_mis(file.split('/')[5]),
                                                      'DateLoadFile' : datetime.datetime.now(),
                                                      'InOrOut'      : 'IN'}, ignore_index=True)
                                break
                    if not len(df):
                        stat = stat.append({'MOName'       : organization,
                                              'NameFile'     : file,
                                              'CountRows'    : 0,
                                              'TextError'    : 'Файл не удалось прочитать',
                                              'OtherFiles'   : other_files,
                                              'mis'          : org_mis(file.split('/')[5]),
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
                else:
                    stat = stat.append({'MOName'       : organization,
                                                  'NameFile'     : file,
                                                  'CountRows'    : len(df),
                                                  'TextError'    : 'Файл прочитан, но он уже был когда-то обработан',
                                                  'OtherFiles'   : other_files,
                                                  'mis'          : org_mis(file.split('/')[5]),
                                                  'DateLoadFile' : datetime.datetime.now(),
                                                  'InOrOut'      : 'IN'}, ignore_index=True)
        else:
            df.columns=names_new
            stat = stat.append({'MOName'       : organization,
                                              'NameFile'     : file,
                                              'CountRows'    : len(df),
                                              'TextError'    : 'Файл прочитан без проблем',
                                              'OtherFiles'   : other_files,
                                              'mis'          : org_mis(file.split('/')[5]),
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
    
        if len(df):
            if len(mo.loc[mo['ftp_user'] == file.split('/')[5], 'level1_key']):
                df['LPU_level1_key'] = mo.loc[mo['ftp_user'] == file.split('/')[5], 'level1_key'].values[0]
                key = df['LPU_level1_key']
                df.drop(labels=['LPU_level1_key'], axis=1,inplace = True)
                df.insert(0, 'LPU_level1_key', key)
                list_.append(df)
                remove_files.append(file)
                #new_file = file.rsplit('/',2)[0] + '/Архив/время_'+ datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') + '.xlsx'
                #df.to_excel(new_file, index=False)
        df = df[0:0]
    
    stat.to_sql('JrnLoadFiles',con,schema='logs',if_exists='append',index=False)
    try:
        svod = pd.concat(list_)
    except ValueError:
        svod_file = get_dir('regiz_svod') + '/' + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') +' свод номеров для проверки.xlsx'
        with pd.ExcelWriter(svod_file) as writer:
            stat.to_excel(writer,sheet_name='статистика',index=False)
        send_file('info',svod_file)
        return svod_file
    else:
        svod.index = range(1,len(svod)+1)
        svod = svod.apply(lambda x: x.loc[::].str[:255] )
        svod.to_sql('TempTableFromMO',con,schema='dbo',if_exists='append',index=False)
        sql_execute('EXEC [dbo].[Insert_Table_FileMO]')
        svod_file = get_dir('regiz_svod') + '/' + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') +' свод номеров для проверки.xlsx'
        with pd.ExcelWriter(svod_file) as writer:
            svod.to_excel(writer,sheet_name='номера',index=False)
            stat.to_excel(writer,sheet_name='статистика',index=False)
    
        send_file('info',svod_file)
        for file in remove_files:
            new_file = file.rsplit('/',2)[0] + '/Архив/время_' + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') + '.' + file.rsplit('.',1)[1]
            try:
                os.replace(file,new_file)
            except:
               pass
        return svod_file 

def regiz_load_to_base_new_old(a):
    sql_execute("""
                TRUNCATE TABLE dbo.TempTableFromMO
                TRUNCATE TABLE nsi.Organization
            """)
    mo = pd.read_excel(get_dir('regiz_svod') + '/mo_directory.xlsx')
    names = ['Номер истории болезни','Дата открытия СМО','Признак амбулаторного СМО','СНИЛС врача']
    path = get_dir('regiz') + '/ori.regiz*/_Входящие/*.xls' 
    files = glob.glob(path) + glob.glob(path + 'x')
    list_ = []
    remove_files = []
    statistic = pd.DataFrame()
    df = pd.DataFrame()

    def read_file(file):
        other_files = str(glob.glob(file.rsplit('/',1)[0] + '/*'))
        if len(mo.loc[mo['ftp_user'] == file.split('/')[5], 'MO']): 
            organization = mo.loc[mo['ftp_user'] == file.split('/')[5], 'MO'].values[0]
        else:
            organization = 'Не определена'
        try:
            df = pd.read_excel(file)
        except:
            try:
                df = pd.read_html(file)
                df = pd.concat(df)
                return df
            except:
                statistic.append({'MOName'       : organization,
                                              'NameFile'     : file,
                                              'CountRows'    : 0 ,
                                              'TextError'    : 'Файл не читается совсем',
                                              'OtherFiles'   : other_files,
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
                return pd.DataFrame(None)
        else:
            if len(df) < 2:
                statistic.append({'MOName'       : organization,
                                              'NameFile'     : file,
                                              'CountRows'    : 0 ,
                                              'TextError'    : 'Файл пустой',
                                              'OtherFiles'   : other_files,
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
                return pd.DataFrame(None)
            if list(df.columns) == ['Unnamed: 0','LPU_level1_key','HistoryNumber','OpenDate','IsAmbulant','SnilsDoctor']:
                del df['Unnamed: 0']
                return df
            if list(df.columns) == ['Номер истории болезни','Дата открытия СМО','Признак амбулаторного СМО','СНИЛС врача']:
                return df
            check = check_table(file,names)
            if not check[0]:
                df = pd.read_excel(file,header=check[3], usecols = check[2], dtype=str)
                del df['Unnamed: 0']
                return df
            else:
                statistic.append({'MOName'       : organization,
                                              'NameFile'     : file,
                                              'CountRows'    : 0 ,
                                              'TextError'    : chek[1],
                                              'OtherFiles'   : other_files,
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
                return pd.DataFrame(None)
    def load_file(df,file): 
        if len(mo.loc[mo['ftp_user'] == file.split('/')[5], 'MO']): 
            organization = mo.loc[mo['ftp_user'] == file.split('/')[5], 'MO'].values[0]
        else:
            organization = 'Неизвестно'
        
        if list(df.columns) == ['LPU_level1_key','HistoryNumber','OpenDate','IsAmbulant','SnilsDoctor']:
            df.to_sql('TempTableFromMO',con,schema='dbo',if_exists='append',index=False)
            statistic.append({'MOName'       :  organization,
                                              'NameFile'     : file,
                                              'CountRows'    : len(df) ,
                                              'TextError'    : 'Успешно загружен повторно',
                                              'OtherFiles'   : '',
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
            return 1

        for col in df.columns:
            df[col.replace(' ','')] = df[col]
            del df[col]

                   
        if len(mo.loc[mo['ftp_user'] == file.split('/')[5], 'level1_key']):
            df['level1_key'] = mo.loc[mo['ftp_user'] == file.split('/')[5], 'level1_key'].values[0]
            key = df['level1_key']
            df.drop(labels=['level1_key'], axis=1,inplace = True)
            df.insert(0, 'level1_key', key)
            df.rename(columns = {        'level1_key'              : 'LPU_level1_key'
                                        ,'Номеристорииболезни'     : 'HistoryNumber'
                                        ,'ДатаоткрытияСМО'         : 'OpenDate'
                                        ,'ПризнакамбулаторногоСМО' : 'IsAmbulant'
                                        , 'СНИЛСврача'             : 'SnilsDoctor'
                                    }, inplace = True)
            try:
                df.to_sql('TempTableFromMO',con,schema='dbo',if_exists='append',index=False)
            except:
                statistic.append({'MOName'       : organization,
                                              'NameFile'     : file,
                                              'CountRows'    : 0 ,
                                              'TextError'    : 'Не удалось загрузить файл в базу',
                                              'OtherFiles'   : '',
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
                return 1
            else:
                statistic.append({'MOName'       : organization ,
                                              'NameFile'     : file,
                                              'CountRows'    : len(df) ,
                                              'TextError'    : 'Успешно загружен',
                                              'OtherFiles'   : '',
                                              'DateLoadFile' : datetime.datetime.now(),
                                              'InOrOut'      : 'IN'}, ignore_index=True)
                
                new_file = file.rsplit('/',2)[0] + '/Архив/время_'+ datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') + '.xlsx'
                df.to_excel(new_file, index=False)
                remove_files.append(file)
                list_.append(df)
                return 1
        else:
            statistic.append({'MOName'       : organization,
                                          'NameFile'     : file,
                                          'CountRows'    : 0 ,
                                          'TextError'    : 'Не определен левел 1 кей',
                                          'OtherFiles'   : '',
                                          'DateLoadFile' : datetime.datetime.now(),
                                          'InOrOut'      : 'IN'}, ignore_index=True)
            return 1
    for file in files:
        try:
            df = read_file(file)
        except:
            send('', 'Ошибка при чтении ' +  file)
        if len(df):
            try:
                load_file(df,file)
            except:
                send('', 'Ошибка при загрузке ' +  file)

    send('','Приступаем к сборке свода')
    
    mo.rename(columns = { 'ftp_user':'ftp_user'
                        , 'oid':'OID'
                        , 'level1_key':'level1_key'
                        , 'МО_краткое наименование':'MONameSpr64'
                        , 'MO':'MOName'
                        , 'MO_полное':'MONameFull'
                        , 'email':'Email'
                        , 'active':'IsActive'
                        }, inplace = True)
    try:
        svod = pd.concat(list_)
    except ValueError:
        mo.to_sql('Organization',con,schema='nsi',if_exists='append',index=False)
        statistic.to_sql('JrnLoadFiles',con,schema='logs',if_exists='append',index=False)
        try:
            f = open(get_dir('regiz_svod') + '/log.txt', 'a')
        except:
            pass
        else:
            with f:
                f.write('Региз нет новых файлов ' + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') + '\n' )
        svod_file = get_dir('regiz_svod') + '/' + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') +' свод номеров для проверки.xlsx'
        with pd.ExcelWriter(svod_file) as writer:
            statistic.to_excel(writer,sheet_name='статистика',index=False)
        return svod_file 
    else:
        svod.index = range(1,len(svod)+1)
        svod_file = get_dir('regiz_svod') + '/' + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') +' свод номеров для проверки.xlsx'
        with pd.ExcelWriter(svod_file) as writer:
            svod.to_excel(writer,sheet_name='номера',index=False)
            statistic.to_excel(writer,sheet_name='статистика',index=False)
        
        mo.to_sql('Organization',con,schema='nsi',if_exists='append',index=False)
        statistic.to_sql('JrnLoadFiles',con,schema='logs',if_exists='append',index=False)
        sql_execute('EXEC [dbo].[Insert_Table_FileMO]')

        try:
            f = open(get_dir('regiz_svod') + '/log.txt', 'a')
        except:
            pass
        else:
            with f:
                f.write("  Региз файлы загружены в базу " + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') + '\n' )
        for file in remove_files:
            try:
                os.remove(file)
            except:
               pass
        return svod_file
                
def regiz_load_to_base_old(a):
    mo = pd.read_excel(get_dir('regiz_svod') + '/mo_directory.xlsx')
    names = ['Номер истории болезни','Дата открытия СМО','Признак амбулаторного СМО','СНИЛС врача']
    path = get_dir('regiz')
    try:
        sql_execute("""
                TRUNCATE TABLE [dbo].[TempTableFromMO]
                TRUNCATE TABLE [nsi].[Organization] 
                """)
    except: 
        raise my_exception('Не удалось очистить таблицы')

    list_=[]
    svod = pd.DataFrame()
    statistic = pd.DataFrame()

    for i in range(len(mo)):
        path_otc = path + '/' + mo.at[i,'ftp_user'] +'/_Входящие'
        if os.path.exists(path_otc) == True:
            listOfFiles =  os.listdir(path_otc)
            files = glob.glob(path_otc+'/*.xl*')
            file_alien = str(glob.glob(path_otc+'/*[!xls]*')).replace(path,'')
            #send('','я работаю')
            if len(files) == 0:
                k = len(statistic)
                statistic.at[k,'MOName'] = mo.at[i,'MO']
                statistic.at[k,'NameFile'] = ''
                statistic.at[k,'CountRows'] = 0
                statistic.at[k,'TextError'] = 'Файл не найден'
                statistic.at[k,'OtherFiles'] = file_alien
                statistic.at[k,'DateLoadFile'] = datetime.datetime.now()
                statistic.at[k,'InOrOut'] = 'IN'

            else:
                for file in files:
                    check = check_table(file,names)
                    if check[0] == 1:
                        k = len(statistic)
                        statistic.at[k,'MOName'] = mo.at[i,'MO']
                        statistic.at[k,'NameFile'] = file.split('/')[-1]
                        statistic.at[k,'CountRows'] = 0
                        statistic.at[k,'TextError'] = check[1]
                        statistic.at[k,'OtherFiles'] = file_alien
                        statistic.at[k,'DateLoadFile'] = datetime.datetime.now()
                        statistic.at[k,'InOrOut'] = 'IN'
                    else:
                        try:
                            otchet = pd.read_excel(file,header=check[3], usecols = check[2], dtype=str)
                        except:
                            k = len(statistic)
                            statistic.at[k,'MOName'] = mo.at[i,'MO']
                            statistic.at[k,'NameFile'] = file.split('/')[-1]
                            statistic.at[k,'CountRows'] = 0
                            statistic.at[k,'TextError'] = 'Не смог прочитать'
                            statistic.at[k,'OtherFiles'] = file_alien
                            statistic.at[k,'DateLoadFile'] = datetime.datetime.now()
                            statistic.at[k,'InOrOut'] = 'IN'
                        else:
                            for col in otchet.columns:
                                otchet[col.replace(' ','')] = otchet[col]
                            otchet['level1_key'] = mo.at[i,'level1_key']
                            otchet.rename(columns = {     'Номеристорииболезни':'HistoryNumber'
                                                          ,'ДатаоткрытияСМО':'OpenDate'
                                                          ,'ПризнакамбулаторногоСМО':'IsAmbulant'
                                                          , 'СНИЛСврача':'SnilsDoctor'
                                                          , 'level1_key':'LPU_level1_key'
                                                     }, inplace = True)
                            columnsTitles=['LPU_level1_key','HistoryNumber','OpenDate','IsAmbulant','SnilsDoctor']
                            otchet=otchet.reindex(columns=columnsTitles)
                            list_.append(otchet)
                            try:
                                otchet.to_sql('TempTableFromMO',con,schema='dbo',if_exists='append',index=False)
                            except:
                                k = len(statistic)
                                statistic.at[k,'MOName'] = mo.at[i,'MO']
                                statistic.at[k,'NameFile'] = file.split('/')[-1]
                                statistic.at[k,'CountRows'] = 0
                                statistic.at[k,'TextError'] = 'Не смог отправить в базу'
                                statistic.at[k,'OtherFiles'] = file_alien
                                statistic.at[k,'DateLoadFile'] = datetime.datetime.now()
                                statistic.at[k,'InOrOut'] = 'IN'
                            else:
                                send('', 'обработан файл\n' + file)
                                new_file = path +'/' + mo.at[i,'ftp_user'] + '/Архив/время_'+ datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') + '.xlsx'
                                otchet.to_excel(new_file)
                                try:
                                    os.remove(file)
                                except PermissionError:
                                    pass
                                k = len(statistic)
                                statistic.at[k,'MOName'] = mo.at[i,'MO']
                                statistic.at[k,'NameFile'] = file.split('\\')[-1]
                                statistic.at[k,'CountRows'] = len(otchet)
                                statistic.at[k,'TextError'] = ''
                                statistic.at[k,'OtherFiles'] = file_alien
                                statistic.at[k,'DateLoadFile'] = datetime.datetime.now()
                                statistic.at[k,'InOrOut'] = 'IN'

    try:
        svod = pd.concat(list_)
    except ValueError:
        #send('info', 'Нет новых файлов для сбора')
        try:
            f = open(get_dir('regiz_svod') + '/log.txt', 'a')
        except:
            pass
        else:
            with f:
                f.write("  Региз нет новых файлов " + str(datetime.datetime.now()) + '\n' )
        raise my_except('Нечего делать')
    else:
        svod.index = range(1,len(svod)+1)
        svod_file = path + '/свод/' + datetime.datetime.now().strftime('%d.%m.%Y_%H-%M') +' свод номеров для проверки.xlsx'
        with pd.ExcelWriter(svod_file) as writer:
            svod.to_excel(writer,sheet_name='номера',index=False)
            statistic.to_excel(writer,sheet_name='статистика',index=False)
        
        mo.to_sql('Organization',con,schema='nsi',if_exists='append',index=False)

        sql_execute('EXEC [dbo].[Insert_Table_FileMO]')

        try:
            f = open(get_dir('regiz_svod') + '/log.txt', 'a')
        except:
            pass
        else:
            with f:
                f.write("  Региз файлы загружены в базу " + str(datetime.datetime.now()) + '\n' )
    
    statistic.to_sql('JrnLoadFiles',con,schema='logs',if_exists='append',index=False)
    return svod_file

#regiz_load_to_base(None)
