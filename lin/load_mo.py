

import pandas as pd
import datetime,warnings,shutil,sqlalchemy,os,glob
from loader import get_dir
from sending import send
from sqlalchemy.orm import sessionmaker

server  = os.getenv('server_parus')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser') # Тут правильный двойной слеш!
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db_ncrn')

eng = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}",pool_pre_ping=True)
con = eng.connect()


mo = pd.read_excel(get_dir('regiz_svod') + '/mo_directory.xlsx')

mo.rename(columns = {'ftp_user':'ftp_user'
                        , 'oid':'OID'
                        , 'level1_key':'level1_key'
                        , 'МО_краткое наименование':'MONameSpr64'
                        , 'MO':'MOName'
                        , 'MO_полное':'MONameFull'
                        , 'email':'Email'
                        , 'active':'IsActive'
                        , 'ИОГВ' : 'IOGV'
                        }, inplace = True)
#print(mo.columns)
mo.to_sql('Organization',con,schema='nsi',if_exists='append',index=False)
