# Телеграм-Бот для автоматизации работ с файлами и базами данных 
# для нужд СПБ ГБУЗ МИАЦ

# Оглавление
<!--ts-->
* [Общие слова](#Общие-слова)  
* [Разбор основного файла main.py](#Файл-main)
   * [Class user](#Class-user)
   * [Class command](#Class-command)
   * [Процедура create_tred](#Процедура-create_tred)
   * [Процедура get_text_messages](#Процедура-get_text_messages)
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
 
 | Переменная  | Тип |  Значение |
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
  
  
  
   
   
