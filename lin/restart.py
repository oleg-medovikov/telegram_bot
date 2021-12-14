import os

status = os.system("/usr/bin/systemctl status telegram-bot.service")
if 'except' in str(status) or 'raise' in str(status) or 'Error' in str(status):
    os.system("/usr/bin/systemctl restart telegram-bot.service")
else:
    print('ok')
