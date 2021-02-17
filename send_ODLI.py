import requests,json,os,pyodbc,base64,random
from collections import namedtuple
from pylatex import Document, PageStyle, Head, Foot, MiniPage, Package, Section, NewLine, HFill, \
                   StandAloneGraphic, MultiColumn, Tabu, LongTabu, LargeText, MediumText, TextColor, \
                    LineBreak, NewPage, Tabularx, TextColor, simple_page_number,HorizontalSpace
from pylatex.utils import bold, NoEscape
from pylatex.base_classes import Environment, Arguments
from loader import get_dir

# создаем подключение
conn = pyodbc.connect(os.getenv('sql_conn'))
cursor = conn.cursor()

# формируем запрос к базе
sql_files = """SELECT 
     [Name_Lab],[ID],[StructureDefinition],[Organization],[lab_familia],[lab_name]
    ,[lab_secondname],[lab_snils],[lab_doljnost],[lab_spech],[OID_Lab_User],[OID_Lab]
    ,[OID_Struct],[Surname],[Firstname],[Patronymic],[Sex],[Birthday],[Passport],[Passport_vidan]
    ,[Policy_OMS],[Policy_OMS_organization],[Snils],[Address_Registration],[Address_Residence],[Phone]
    ,[Date_Test],[Med_Service],[Code_Test],[Type_Material],[Result_Test]
    ,[GUID_Bundle],[GUID_Patient],[GUID_Observation],[GUID_Order]
    ,[GUID_DiagnosticReport],[GUID_OrderResponse],[GUID_Binary],[GUID_Practitioner]
            FROM [COVID].[odli].[For_sending_into_ODLI] ORDER BY [ID]
"""
# формируем именнованный кортеж, для строк
stroka = namedtuple ('stroka', [
    'Name_Lab','ID','StructureDefinition','Organization','lab_familia','lab_name'
    ,'lab_secondname','lab_snils','lab_doljnost','lab_spech','OID_Lab_User','OID_Lab'
    ,'OID_Struct','Surname','Firstname','Patronymic','Sex','Birthday','Passport','Passport_vidan'
    ,'Policy_OMS','Policy_OMS_organization','Snils','Address_Registration','Address_Residence','Phone'
    ,'Date_Test','Med_Service','Code_Test','Type_Material','Result_Test'
    ,'GUID_Bundle','GUID_Patient','GUID_Observation','GUID_Order'
    ,'GUID_DiagnosticReport','GUID_OrderResponse','GUID_Binary','GUID_Practitioner'
    ])

# Сведения о МИС
MIS_name = get_dir('MIS_name')
MIS_number = get_dir('MIS_number')

# создаем список где будут храниться строки
db_lab = []

# мое исключение
class my_except(Exception):
    pass

# Процедура для забора исследований, которые необходимо отправить
def load_data_research():
    db_lab.clear()
    for row in cursor.execute(sql_files):
        db_lab.append(stroka(*row))

class ExampleEnvironment(Environment):
    _latex_name = 'exampleEnvironment'
    packages = [Package('mdframed')]

def send_otvet_id(id,otvet):
    sql = f"""
    update [odli].[History_Files_Laboratory] 
    set [IdOrder] = '{otvet}'
    where ID = {id}
    """
    cursor.execute(sql)
    cursor.commit() 

def generate_pdf(stroka):
    pdf_path = get_dir('odli_pdf') + '\\' +stroka.Surname +' '+ stroka.Firstname +' '+ stroka.Patronymic
    logo_image = r'miaclogo.png'

    pdf = Document(pdf_path)
    pdf.packages.add(Package('babel',options='russian'))
    pdf.packages.add(Package('pdfx', options= NoEscape('a-1b') ))
    pdf.packages.add(Package('inputenc',options='utf8'))
    pdf.packages.add(Package('fontenc',options='T2A'))
    pdf.packages.add(Package('geometry',options='a5paper'))

    first_page = PageStyle("firstpage")
    with first_page.create(Head("L")) as header_left:
        with header_left.create(MiniPage(width=NoEscape(r"0.49\textwidth"),pos='l')) as logo_wrapper:
            logo_wrapper.append(StandAloneGraphic(image_options="width=120px",filename=logo_image))
        with header_left.create(MiniPage(width=NoEscape(r"0.49\textwidth"),pos='c')) as logo_wrapper:
            logo_wrapper.append(NoEscape("Сгенерированно в СПб ГБУЗ МИАЦ"))
            logo_wrapper.append(NewLine())
            logo_wrapper.append(NoEscape("В основе данные, предоставленные лабораториями"))
    pdf.preamble.append(first_page)

    pdf.change_document_style("firstpage")
    pdf.add_color(name="lightgray", model="gray", description="0.80")
    pdf.append(HorizontalSpace(size="500px"))
    with pdf.create(Section(NoEscape("Исследование на COVID-19"),numbering=False)):
        pdf.append(NoEscape("Наименование лаборатории: " + stroka.Name_Lab ))
        pdf.append(NewLine())
        pdf.append(NoEscape("Дата исследования: " + stroka.Date_Test))
        pdf.append(NewLine())
        pdf.append(NoEscape("Ответственный за исследование: "))
        pdf.append(NewLine())
        pdf.append(NoEscape(stroka.lab_familia +' '+ stroka.lab_name +' '+ stroka.lab_secondname))
    with pdf.create(Section(NoEscape("Пациент: ") ,numbering=False)):
        pdf.append(LargeText(NoEscape(stroka.Surname +' '+ stroka.Firstname +' '+ stroka.Patronymic)))
        pdf.append(NewLine())
        pdf.append(NewLine())
        pdf.append(NoEscape("Дата рождения: " + stroka.Birthday))
        pdf.append(NewLine())
        pdf.append(NoEscape("Паспорт: " + stroka.Passport))
        pdf.append(NewLine())
        pdf.append(NoEscape("СНИЛС: " + stroka.Snils))
        pdf.append(NewLine())
        pdf.append(NoEscape("ОМС: " + stroka.Policy_OMS))
        pdf.append(NewLine())
        pdf.append(NoEscape("Контактный номер: " + stroka.Phone))
        pdf.append(NewLine())
    with pdf.create(Section(NoEscape("Результат: "),numbering=False)):
        pdf.append(NoEscape("Качественное обнаружение короновируса SARS 2 в различных образцах: "))
        pdf.append(NewLine())
        pdf.append(NewLine())
        if stroka.Result_Test == 'ND':
            pdf.append(TextColor('green',LargeText(NoEscape('Не обнаружено'))))
        if stroka.Result_Test == 'DET':
            pdf.append(TextColor('red',LargeText(NoEscape('Обнаружено'))))

    pdf.generate_pdf(clean_tex=True,compiler='pdflatex')

    with open(pdf_path+'.pdf', "rb") as pdf_file:
        encoded_pdf = base64.b64encode(pdf_file.read()).decode()
    return encoded_pdf

def generate_json_new(stroka,encoded_pdf):
    json = {
    "resourceType": "Bundle",
    "meta": {
        "profile": [
            "StructureDefinition/" + stroka.GUID_Bundle
                    ]
            },
    "type": "transaction",
        "entry": [{
            "fullUrl": "urn:uuid:" + stroka.GUID_Order ,
                "resource": {
                    "resourceType": "Order",
                    "source": {
                        "reference": "Organization/" + stroka.Organization
                                },
                    "target": {
                        "reference": "Organization/" + stroka.Organization
                                },
                    "detail": {
                        "reference": ""
                                }
                            },
        "request": {
            "method": "POST",
            "url": "Order"
                    }
                },{
        "fullUrl": "urn:uuid:" + stroka.GUID_OrderResponse,
        "resource": {
            "resourceType": "OrderResponse",
            "identifier": [{
                "system":MIS_number,
                "value": stroka.ID,
                "assigner": {
                    "display" : MIS_name
                            }
                        }],
            "request": {
            "reference": "urn:uuid:" + stroka.GUID_Order
                    },
            "date": stroka.Date_Test,
            "who": {
                "reference": "Organization/" + stroka.Organization
                },
            "orderStatus": "completed",
            "description": "Comment",
            "fulfillment": [{
                "reference": "urn:uuid:" + stroka.GUID_DiagnosticReport,
                            }]
                    },
            "request": {
                "method": "POST",
                "url": "OrderResponse"
                        }
                },{
        "fullUrl": "urn:uuid:" + stroka.GUID_DiagnosticReport, 
        "resource": {
            "resourceType": "DiagnosticReport",
            "meta": {
                "security": [{"code": "N"}]
                    },
            "identifier": [{
                "type": {
                    "coding": [{
                        "system": "urn:oid:1.2.643.2.69.1.1.1.31",
                        "code": "A26.08.014.997", # код услуги поставил всем одинаковый
                        "version": "117"
                                }]
                        },
                "system": "urn:oid:1.2.643.5.1.13.2.7.100.6",
                "value": "123456",
                "assigner":    {
                    "reference": "Organization/" + stroka.Organization,
                    "display": "Эпидномер"
                                }
                            }],
            "category": {
                "coding": [{
                    "system": "urn:oid:1.2.643.5.1.13.13.11.1117",
                    "version": "1",
                    "code": "601"
                            }]
                        },
            "status": "final",
            "code": {
                "coding": [{
                    "system": "urn:oid:1.2.643.2.69.1.1.1.31",
                        "version": "117",
                        "code": "A26.08.014.997"
                            }]
                    },
        "subject": {
            "reference": "urn:uuid:" + stroka.GUID_Patient
                    },
        "effectiveDateTime": stroka.Date_Test,
        "issued": stroka.Date_Test,
        "performer": [{
            "reference": "urn:uuid:" + stroka.GUID_Practitioner
                        }],
        "result": [{
            "reference": "urn:uuid:" + stroka.GUID_Observation
                    }],
        "presentedForm": [{
            "url": "urn:uuid:" + stroka.GUID_Binary
                            }]
                    },
        "request": {
            "method": "POST",
            "url": "DiagnosticReport"
                    }
            },{
        "fullUrl": "urn:uuid:" + stroka.GUID_Patient,
        "resource": {
            "resourceType": "Patient",
            "identifier": [{
                "system": "urn:oid:1.2.643.5.1.13.2.7.100.5",
                "value": MIS_name,
                "assigner":{
                    "display" : "1.2.643.2.69.1.2.27"
                    }
                            },{
                "system": "urn:oid:1.2.643.2.69.1.1.1.6.14",
                "value": stroka.Passport,
                "assigner": {
                          "display": stroka.Passport_vidan # кем выдан паспорт
                            }
                            },
                            {
                "system": "urn:oid:1.2.643.2.69.1.1.1.6.223",
                "value": stroka.Snils,
                "assigner": {
                    "display": "ПФР"
                            }
                            },
                            {
                "system": "urn:oid:1.2.643.2.69.1.1.1.6.228",
                "value": stroka.Policy_OMS,
                "assigner": {
                    "display": stroka.Policy_OMS_organization
                            }
                            }
                            ],
            "name": [{
                "family": [stroka.Surname,stroka.Patronymic],
                "given": [stroka.Firstname]
                    }],
            "gender": stroka.Sex,
            "birthDate": stroka.Birthday,
            "address": [{
                "use": "home",
                "text": stroka.Address_Registration
                        },{
                "use": "temp",
                "text": stroka.Address_Residence
                        }
                        ],
            "managingOrganization": {
                "reference": "Organization/" + stroka.Organization
                                    }
                    },
        "request": {
            "method": "POST",
            "url": "Patient"
                    }
            },{
        "fullUrl": "urn:uuid:" + stroka.GUID_Practitioner,
        "resource": {
            "resourceType": "Practitioner",
            "identifier": [{
                "system": "urn:oid:1.2.643.5.1.13.2.7.100.5",
                "value": MIS_name,
                "assigner":{
                    "display" : MIS_number
                    }
                        },{
                "system": "urn:oid:1.2.643.2.69.1.1.1.6.223",
                "value": stroka.lab_snils,
                "assigner": {
                    "display": "ПФР"
                            }
                        }],
            "name": {
                "family": [stroka.lab_familia,stroka.lab_secondname],
                "given": [stroka.lab_name]
                    },
            "practitionerRole": [{
                "managingOrganization": {
                    "reference": "Organization/" + stroka.Organization
                                        },
            "role": {
                "coding": [{
                    "system": "urn:oid:1.2.643.5.1.13.13.11.1002",
                    "version": "4",
                    "code": stroka.lab_doljnost
                            }]
                    },
            "specialty": [{
                "coding": [{
                    "system": "urn:oid:1.2.643.5.1.13.13.11.1066",
                    "version": "2",
                    "code": stroka.lab_spech 
                            }]
                        }]
                                }]
                    },
        "request": {
            "method": "POST",
            "url": "Practitioner"
                    }
        },{
        "fullUrl": "urn:uuid:" + stroka.GUID_Observation,
        "resource": {
            "resourceType": "Observation",
            "status": "final",
            "interpretation": {
                "coding": [{
                    "system": "urn:oid:1.2.643.5.1.13.13.11.1381",
                        "version": "2",
                        "code": stroka.Result_Test
                            }]
                            },
            "code": {
                "coding": [{
                    "system": "urn:oid:1.2.643.2.69.1.1.1.1",
                    "version": "164",
                    "code": stroka.Code_Test
                            }]
                    },
            "issued": stroka.Date_Test,
            "performer": [{
                "reference": "urn:uuid:" + stroka.GUID_Practitioner
                            }],
                    },
        "request": {
            "method": "POST",
            "url": "Observation"
                    }
        },{
        "fullUrl": "urn:uuid:" + stroka.GUID_Binary,
        "resource": {
            "resourceType": "Binary",
            "contentType": "application/pdf",
            "content": encoded_pdf
                    },
        "request": {
            "method": "POST",
            "url": "Binary"
                    }
            }]
    }
    # Удаляем лишние паспортные данные
    delete_elements = []
    for element in json['entry'][3]['resource']['identifier']:
        if element['value'] == '0':
            delete_elements.append(element)
    for delete_element in delete_elements:
        json['entry'][3]['resource']['identifier'].remove(delete_element)
    return json


def send_bundle_to_ODLI(a):
    # загружаем из витрины исследования, которые нужно отправить
    load_data_research()
    if len(db_lab) == 0:
        raise my_except('Некого отправлять, витрина пуста')
    # начинаем отправлять по одному
    for row in db_lab:
        # начнем с генерации PDF исследования
        encoded_pdf = generate_pdf(row)
        #  Адрес АПИ 
        url = get_dir('odli_http_test')
        # Заголовок джейсона
        headers = {'content-type': 'application/json', 'Authorization': get_dir('odli_key_test')}
        # Настройка прокси моего компа
        proxies = {'http': os.getenv('http_proxy')}
        # Создаем джейсон
        json_bundle = generate_json_new(row,encoded_pdf)
        # на всякий случай сохраняю бандл в файлик
        data=json.dumps(json_bundle,ensure_ascii=False)
        with open(get_dir('odli_pdf') + r'\data.json', 'w',encoding='utf-8') as f:
            json.dump(data,f,ensure_ascii=False)
        # отправка бандла на сервера
        response = requests.post(url, data=data.encode('utf-8'), headers=headers, proxies=proxies)
        # чтение ответа
        otvet = response.json()
        # отправляем id бандла, которое получили от сервера в базу
        send_otvet_id(int(row.ID),otvet["id"])
        # так как это тест, то остановимся на 1 исследовании
        return 'успешно отправлен бандл ' + otvet["id"]
    return 'Было отправлено ' + len(db_lab) + ' бандлов'




