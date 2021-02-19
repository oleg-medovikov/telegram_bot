# Телеграм-Бот для автоматизации работ с файлами и базами данных 
# для нужд СПБ ГБУЗ МИАЦ

# Оглавление
<!--ts-->
* [Общие слова](#Общие-слова)  
* [Разбор основного файла main.py](#Файл-main)
    * [Class user](#Class-user)
    * [Class command](#Class-command)
    * [Процедура create_tred](#Процедура-create_tred)
    * * [Процедура get_text_messages](#Процедура-get_text_messages)
* [Разбор модуля loader.py](#loader)
    * [Процедура получения директории](#процедура-get_dir) 
    * [Процедура проверки таблицы](#процедура-check_table)
    * [Процедура поиска файла](#Процедура-search_file)
    * [Процедура проверки файла](#Процедура-check_file)
    * [Процедура сложения ФР](#Процедура-slojit_fr)
    * [Процедура конвертации Excel в CSV](#Процедура-excel_to_csv)
    * [Процедура загрузки ФР](#Процедура-load_fr)
    * [Процедура загрузки ФР умершие](#Процедура-load_fr_death)
    * [Процедура загрузки ФР лаборатория](#Процедура-load_fr_lab)
    * [Процедура загрузки УМСРС](#Процедура-load_UMSRS)
    * [Процедура загрузки ВП и COVID](#Процедура-load_report_vp_and_cv)
<!--te-->



## Общие слова
### Реализованные фичи:
 1. Работа с пользователями, разграничение прав пользователей по группам.
 2. Запуск процедур по расписанию.
 3. Запуск процедур отдельным потоком, в случае падения которого, оповещение пользователя и админа сообщением с ошибкой.
 4. Оповещения админа о выполнении процедуры конретным пользователем.
 5. Возможность предоставления sql запросов с компактной итоговой таблицей в виде PNG изображения таблицы, для удобства при работе с телефона.
 6. Конвертация xlsx в csv.
 7. Процедура проверки файла в виде поиска шапки таблицы внутри файла excel с проверкой названий колонок и их порядка.
 8. Генерация PDF/A файлов стандарта a-2b с использованием Latex
 
### Типы решаемых ботом задач:
 1. Создание сводных отчетов путем сбора excel файлов из конкретной директории или списка директорий.
 2. Генерация файлов частных отчетов для медицинских организаций и разложение по списку директорий.
 3. Заполнение готовых шаблонов файлов excel данными из других файлов или баз данных.
 4. Создание отчетов на основе витрин баз данных или процедур обращения к базам данных.
 5. Выполнение диагностических запросов к базе данных и отображение в удобном виде.
 6. Создание презентации на основе запросов к базе данных.
 7. Загрузка данных в базу данных из файла с предварительной обработкой.
 8. Генерация json бандла результата исследования без заявки на основе данных из базы данных и отправка на сервера ОДЛИ.
 

## Файл main

### Class user

Список пользователей users заполняется объектами user из sql таблицы с помощью процедуры user.get_users(). В состав объекта входят:

| Переменная | Тип |  Значение |
| ------------- | ------------- | ------------- | 
| user_id  | int | идентификатор пользователя телеграм, которое можно узнать, если неизвестный пользователь напишет боту сообщение |
| short_name | str | имя и отчество пользователя, используется для обращения к пользователю|
| groups | str | определяет принадлежность к группе пользователей, на данный момент группа одна на пользователя |
 
Процедуры класса:

| Процедура   | Аргументы |  Значение |
| ------------- | ------------- | ------------- |
|get_users | no | Предназначена для заполнения списка users объектами класса user с помощью sql запроса к таблице robo.bot_users | 
|known| id | Проверяет, является ли данный id известным и возвращает True/False |
|name| id | Возвращает имя и отчетсво по id в виде строки|
|master| no | Возвращает id главного пользователя  |


### Class command

Список комманд доступных для выполнения ботом, заполняется результатом sql процедуры с помощью процедуры command.get_commands() 

| Переменная  | Тип |  Значение |
| ------------- | ------------- | ------------- | 
|command_id| int | Идентификатор команды, совпадает с номером объекта в списке commands |
|command_name|str| Имя команды, которое отображается для пользователя |
|command_key|list| Список ключей, которыми можно запустить команду, разделитель , |
|procedure_name|str| Название процедуры, которую запустит комманда |
|procedure_arg|str| Аргументы процедуры |
|return_file|bool| флаг, воазвращает ли процедура файл? |
|hello_words|list| Список фраз приветсвия, одна из которых случайно выбирается перед выполнением задания |
|ids|list| Список id пользователей, которым разрешено выполнять данную команду|

Процедуры класса:

| Процедура   | Аргументы |  Значение |
| ------------- | ------------- | ------------- |
|get_commands|no| Заполняет список commands объектами command |
|search|key| Ищет в списке комманд команду в списке ключей которой есть данный ключ (сообщение пользователя), возвращает объект command |
|check_id|id,command|Проверяет есть ли данный id в свиске разрешенных id для данной команды |
|hello_mess|id| Генерирует строку с перечнем разрешенных для данного id команд |
|number|id,mess| Объединение функций search и check_id в одну, ищет комманду по ключу и проверяет права на выполнение |

### Процедура create_tred
 
Работает с названием процедуры, переводя объект строка в объект фунции, сделано так, как названия запускаемых процедур берутся из sql таблиц. 
Основная задача процедуры - это создание отдельного треда для выполнения функции, после завершения которого она должна вернуть флаг успеного выполнения и результат выполнения или текст исключения, которое случилось при выполнении.

### Процедура go

Запускает бесконенчный цикл для работы расписания, на основе библиотеки schedule.
Запускается отдельным тредом, все задания внутри расписания запускаются отдельными тредами через create_tred для избежания падения расписания.

### Процедура get_hello_start

Определяет по текущему времени часть дня и возращает нужное приветсвие, например, "Доброе утро" или "Добрый вечер".

### Процедура get_text_messages

Основная процедура бота, обрабатывающая входящие сообщения в виде объектов message, работает в бесконечном цикле.

Логика обработки сообщений:
1. Получаем сообщение в виде message
2. С помощью user.known(message.from_user.id) определяем, является ли отправитель извесным пользователем. Если нет, то отправляем админу сообщение о неизвесном пользователе с его id.
3. Если сообщение в виде 'привет', 'ghbdtn','/start','старт', то приветствуем отправителя и показываем ему перечень доступных ему команд.
4. В обратном случае, с помощью  command.number(message.from_user.id,message.text.lower()) пробуем определить номер команды. При неудаче перенаправляем сообщение админу.
5. Если удалось определить номер запрашиваемой команды, то отправителю посылавется случайное приветсвие, соответсвующее команде, и запускается сама команда
6. Если при выполнении команды произошло падение (result[0] == False) отправляем текст исключения отправителю и админу
7. При удачном выполнении команды проверяем, должен ли быть возвращен файл
8. Возвращаем отправителю результат в виде файла или текстового сообщения
9. Уведомляем админа об успешном выполнении задачи

## loader

### процедура get_dir

Необходима для работы с часто используемыми директориями и другими константами, которые можно хранить, как строки. Замена глобальным переменным, так как работать с sql таблицей проще. Возращает строку из таблицы robo.directions_for_bot через имя переменной. В самой таблице хранится краткое описание, для чего нужна данная директория.

### процедура check_table

Проверяет sql таблицу на наличие строк с сегоднящней датой создания, что потверждает загрузку новых данных.

### Процедура search_file

Проверяет наличие актуальных файлов выгрузки в директории ROBOT. Ищет одновременно xlsx и csv. Предпочитает csv. Возвращает 2 флага и директорию. Первый флаг о нахождении xlsx, второй - csv.

### Процедура check_file

Процедура проверяет файл, ищет расположение шапки таблицы и сверяет названия колонок со списком названий. Возвращает флаг проверки, текст ошибки (перечень не найденных колонок), список с номерами найденных колонок и номер строки, где расположен заголовок таблицы. Так как для работы процедуры необходимо прочесть с файл, она работает с CSV файлами, полученными после конвертации xlsx, что в разы ускоряет чтение файла скриптом даже с учетом конвертации.

Процедура на данный момент работает с 4 файлами:
- Выгрузка Федерального регистра COVID-19
- 33 отчет по умершим
- 34 отчет по лабораторным исследованиям
- выгрузка УМСРС

Логика процедуры:
1. Определиться со списком наименований колонок names, за что отвечает переменная category. 
2. Посчитать сумму колонок len(names), которых необходимо найти, если будет найдено меньше колонок, то функция возратит ошибку.
3. Попробовать загрузить файл без заголовка в датафрейм file, если произойдет исключение, то возвращаем, что файл не читается.
4. Теперь необходимо каждую строку датафрейма проверить на наличие в ней заголовка таблицы, проходя каждую ячейку и сравнивая ее со списком названий колонок.
5. Цикл выполняется до тех пор, пока число найденных колонок не будет равняться числу суммы колонок или пока не закончатся строки в датафрейме.
6. При сравнивании значений в ячейках со списком убираются пробелы, чтобы количество пробелов не влияло на результат.
7. Если были найдены не все колонки, то из списка всех колонок вычитаем список найденных колонок ' Не найдена колонка ' + str(list(set(names) - set(names_found) ) ) и возвращается строка с ненайдеными колонками. Это как правило зачит, что в шаблоне файла произошли изменения или допущена ошибка в названии столбца.


### Процедура slojit_fr

Так как выгрузка федерального регистра происходит по частям, необходимо сложить части в один файл (на самом деле в 2 разных файла с разным количеством столбцов). Параллельно со складыванием происходит подсчет нескольких цифр:
- всего на стационарном лечении
- всего заболело
- всего умерло
- всего выздоровело за день
Для последней цифры необходимо число за вчерашний день, которое берется из актуальной базы из таблицы robo.values.
Цифры отправляются запустившему процедуру.

### Процедура excel_to_csv

Процедура читает xlsx файл с помощью библиотеки openpyxl и записывает данные в csv стандартными средствами. Рядом расположена процедура excel_to_csv_old, делающая тоже самое, но через библеотеку xlrd. Она быстрее, но форматирует даты как число.

### Процедура load_fr

После обнаружения актуального файла, конвертации и проверки происходит попытка загрузки данных в датафрейм df. После начинает работать процедура fr_to_sql. Операции перед загрузкой в базу:
- удаление строк, где колонка 'Дата создания РЗ' пустая. (это последняя строка с общим количеством строк)
- удаление колонок 'Ведомственная принадлежность' и 'Осложнение основного диагноза'
- расчет числа всего выздоровевших и отправка его в таблицу robo.values
- очистка cv_input_fr
После загрузки датафрейма в cv_input_fr:
- запуск sql процедуры cv_Load_FedReg
- запуск check_table('fedreg') чтобы убедиться в том, что появились новые строки в таблице

### Процедура load_fr_death

После обнаружения актуального файла, конвертации и проверки происходит попытка загрузки данных в датафрейм df. После начинает работать процедура fr_death_to_sql. Операции перед загрузкой в базу: 
- скриптом происходит прохождение по всем ячейкам датафрейма и обрезание строк длиннее 255 символов
- скриптом происходит прохождение по всем ячейкам датафрейма, чтобы убрать 'nan'

После загрузки датафрейма в cv_input_fr_d_all_2 поочередно запускаются sql процедуры:
- Insert_Table_cv_input_fr_d_all_2 
- cv_from_d_all_to_d_covid
- cv_Load_FedReg_d_All
- cv_Load_FedReg_d_covid
  
### Процедура load_fr_lab

После обнаружения актуального файла, конвертации и проверки происходит попытка загрузки данных в датафрейм df. После начинает работать процедура fr_lab_to_sql. Операции перед загрузкой в базу: 
- колонки переименовываются в порядковые номера, так как оригинальные слишком длинные и драйвер sql на них жалуется
- удаляются строки, с пустыми ячейками в 1 - 6 графе, так как это некорректные строки
После загрузки в таблицу cv_input_fr_lab_2 поочередно запускаются sql процедуры:
- Insert_Table_cv_input_fr_lab_2
- cv_load_frlab
И происходит проверка check_table('fedreg_lab')

### Процедура load_UMSRS

После обнаружения актуального файла, конвертации и проверки происходит попытка загрузки данных в датафрейм df. После начинает работать процедура UMSRS_to_sql. Операции перед загрузкой в базу: 
- имена колонок заменяются на порядковые номера при чтении файла, так как имена колонок приравниваются к номерам найденых колонок usecols = check[2], names = check[2]

После загрузки в таблицу cv_input_umsrs_2 запускаются sql процедуры 
- Insert_Table_cv_input_umsrs_2
- cv_Load_UMSRS

И происходит проверка check_table('umsrs')

### Процедура load_report_vp_and_cv

Данная процедура читает ежедневные отчеты медицинских организаций о ВП и ковиде, присылаемые на почту, загружает их в базу и выгружает в виде сводного отчета с проверками.

Логика процедуры:

1. Найти файлы отчетов организаций из директории VP_CV, закончить, если папка пустая.
2. Создать папку с сегодняшней датой
3. Поочередно вытащить данные из файлов, если не выходит прочитать файл, то запускается процедура open_save, которая открывает, пересохраняет файл и пробует прочесть заново.
4. Собрать данные в один датафрейм, переименовать колонки и загрузить его в sql таблицу mon_vp.HistoryFileM.
5. Сделать проверку загруженных данных check_data_table('mon_vp.v_DebtorsReport'), если проверка не пройдена, то вернуть short_report('SELECT * FROM mon_vp.v_DebtorsReport'). В данном случае проверяется, все ли организации предоставили отчет, в случае, если не все, то возвращается картинка со списком должников.
6. Если все организации предоставили отчет, то происходит выгрузка данных из витрины mon_vp.v_GrandReport, которые помещаются в файл шаблона сводного отчета.
7. Выполняется sql процедура проверки mon_vp.p_CheckMonitorVpAndCovid и ее результаты добавляются в шаблон отчета.
8. Готовый сводный файл отправляется пользователю.

