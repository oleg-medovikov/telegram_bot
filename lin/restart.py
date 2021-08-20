import os

status = os.system("/usr/bin/systemctl status telegram-bot.service")
if 'except' in str(status):
    os.system("/usr/bin/systemctl restart telegram-bot.service")
else:
    print('ok')
