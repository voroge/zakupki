import os
# модуль для работы с ftp
import ftplib
# модуль для работы с zip архивами
from zipfile import *
# Библиотека для работы с Oracle
import cx_Oracle
import pandas as pd
import datetime as dt
import time
# !!!Важно. Установленная по умолчанию в Canopy версия библиотеки lxml не умеет читать кодировку windows-1251
# Поэтому необходимо скачать с сайта https://pypi.python.org/pypi/lxml последнюю версию (3.7.3) библиотеки
# Подключаем библиотеку для парсинга XML
import lxml.etree as et
# библиотека для работы с INI подобными файлами
import configparser


class PurchTools(object):

    def __init__(self):
        # Для корректного отображения оракловых сообщений
        os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
        self.gdebug = True
        self.__config = self.__loadconfig()
        self.conn=None
        self.cursor=None

    def log(self, amessage):
        if self.gdebug:
            print(time.asctime(time.localtime(time.time())) + ": " + amessage)
        else:
            try:
                f = open('purch.log', 'a')
                f.write(time.asctime(time.localtime(time.time())) + ": " + amessage + '\n')
            finally:
                f.close()

    def dprint(self, amessage):
        if self.gdebug:
            print(amessage)

    def __loadconfig(self):
        if not os.path.exists('settings.ini'):
            self.log('Ошибка! Не найден конфигурационный файл settings.ini! Дальнейшее выполнение приложения не возможно!')
            exit(1)
            #raise Exception('Can''t fing config file settings.ini!')
        lconfig = configparser.ConfigParser()
        lconfig.read('settings.ini')
        return lconfig

    def ftpconnect(self, afz):
        # создаем объект ftp для заданного адреса ftp-сервера
        lftp = ftplib.FTP(self.__config.get(afz, 'URL'))
        # коннектимся к серверу с именем пользователя и паролем
        lftp.login(self.__config.get(afz, 'user'), self.__config.get(afz, 'pass'))
        return lftp

    # Процедура скачивает файл filename с указанной директории directory ftp и кладет его по указанному локальному пути
    def download(self, ftp, directory, loc_filepath, filename):
        ftp.cwd(directory)  # Задает текущую директорию на ftp
        loc_file_name = os.path.join(loc_filepath, filename)  # конкатенирует имя файла с путем
        # Открывает файл для записи в двоичном формате. Указатель стоит в начале файла.
        # Создает файл с именем имя_файла, если такового не существует.
        f = open(loc_file_name, "wb")
        # Получить файл в двоичном режиме. Команда должна иметь соответствующую команду RETR: 'RETR имя_файла'.
        ftp.retrbinary("RETR " + filename, f.write)
        f.close()

    # Функция возвращает список файлов или папок в указанной директории на ftp
    def get_ftp_dir_list(self, ftp, ftp_path):
        ftp.cwd(ftp_path)  # Задает текущую директорию на ftp
        llist = []
        # Получение файла или каталога в ASCII режиме передачи.
        # Команда должна иметь соответствующую команду RETR (см. retrbinary ()) или команду такую как LIST, NLST или MLSD
        ftp.retrlines("LIST", llist.append)
        # т.к. в полученном списке присутствуют все атрибуты файла или папки, надо вычленить только имя
        llist = [s.split(None, 8)[-1].lstrip() for s in llist]
        return llist

    def unzipall(self, path, archname):
        # Проверяем, является ли указанный файл zip архивом
        if is_zipfile(path + archname):
            # Открываем архив на чтение
            z = ZipFile(path + archname, 'r')
            # Распаковываем все файлы из архива по указанному пути
            z.extractall(path)
            # Возвращаем список файлов в архиве
            return z.namelist()
            z.close()
        else:
            self.log('Некорректный архив : ' + archname)

    def __str_to_date(self, astrdate):
        # Даты в xml пишутся по разному. 2015-01-16T00:00:00 или 2015-01-16
        if len(astrdate) == 0:
            return None
        elif astrdate[0].text[0] == '0':
            return None
        elif ('T' in astrdate[0].text):
            return dt.datetime.strptime(astrdate[0].text, '%Y-%m-%dT%H:%M:%S')
        else:
            return dt.datetime.strptime(astrdate[0].text, '%Y-%m-%d')

    def __xpath_nulls(self, atag_value):
        if len(atag_value) == 0:
            return None
        else:
            return atag_value[0].text

    def __xpath_float(self, atag_value):
        if len(atag_value) == 0:
            return 0
        else:
            return float(atag_value[0].text)

    def __xpath_composite(self, aelements):
        llist = list()
        for index, value in enumerate(aelements):
            if value.text.strip() == '':
                llist.append(dict())
            else:
                llist[len(llist) - 1][value.tag[value.tag.find('}') + 1:]] = value.text.strip()
            if index == len(aelements) - 1:
                dict(llist[len(llist) - 1])
        return llist

    # Функция парсинга xml
    def parse_xml(self, afile, adoc):
        # парсим файл XML и создаем дерево елементов element tree
        etxml = et.parse(afile)
        # получаем корневой элемент дерева
        root = etxml.getroot()
        # Данный цикл позволяет получить список всех элементов (тэгов) дерева, названий и значений
        # Это особенно актуально, когда перед элементом указывается пространство имен (name space), например
        # {http://zakupki.gov.ru/223fz/contract/1}contractRegNumber
        # Это нужно при поиске конкретных тэгов с помощью функций xpath и findall
        # for item in root.iterfind('.//'):
        #    print item.tag + '  :  ' + item.text
        # В аргументе функции xpath указываем строку для поиска, здесь './/' означает,
        # что надо искать не только среди потомков корневого элемента, но во всех элементах дерева вообще
        # также перечисляем все пространства имен, используемые перед названием тэга, их можно взять из заголовка xml
        # они находятся после ключа xmlns= (для тэгов без указания пространства имен, например <inn>5260267654</inn>)
        # или xmlns: (для тэгов с указанием пространтства имен, например <ns2:code>40000</ns2:code> ),
        # сами название пространств имен в xpath не обязательно должны совпадать с xml, но пути и URL должны совпадать
        if adoc == 'purchaseprotocol':
            lnamespaces = {'ns2': 'http://zakupki.gov.ru/223fz/purchase/1', 'ns': 'http://zakupki.gov.ru/223fz/types/1'}
            ldocdata = [
                root.xpath('.//ns2:purchaseInfo/ns:purchaseNoticeNumber', namespaces=lnamespaces)[0].text,
                root.xpath('.//ns2:purchaseInfo/ns:purchaseMethodCode', namespaces=lnamespaces)[0].text,
                root.xpath('.//ns2:purchaseInfo/ns:purchaseCodeName', namespaces=lnamespaces)[0].text,
                self.__xpath_composite(
                    root.xpath(
                        './/ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:name | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:inn | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:kpp | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:ogrn | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:address',
                        namespaces=lnamespaces
                    )
                ),
            ]
        # Альтернативная xpath функция, но с меньшими возможностями и немного отличным синтаксисом поискового запроса
        # lm = root.findall('.//{http://zakupki.gov.ru/223fz/contract/1}contractRegNumber')
        return ldocdata

    def __compdate(self, afname, adfrom, adto):
        ifrom = afname.find('_000000_')
        if ifrom == -1:
            return False
        else:
            ldfile = afname[ifrom + 8:ifrom + 16]
            if adfrom == '*' and adto != '*':
                return dt.datetime.strptime(ldfile, '%Y%m%d') \
                       <= dt.datetime.strptime(adto, '%Y%m%d')
            elif adto == '*' and adfrom != '*':
                return dt.datetime.strptime(adfrom, '%Y%m%d') \
                       <= dt.datetime.strptime(ldfile, '%Y%m%d')
            elif adto == '*' and adfrom == '*':
                return True
            else:
                return dt.datetime.strptime(adfrom, '%Y%m%d') \
                       <= dt.datetime.strptime(ldfile, '%Y%m%d') \
                       <= dt.datetime.strptime(adto, '%Y%m%d')

    def __complists(self, alist1, alist2):
        for i in alist1:
            for j in alist2:
                si = str(i) + '.'
                sj = str(j) + '.'
                if si.find(sj) == 0:
                    return True
        return False

    # Загружаем все файлы с ftp госзакупок из подпапки ftp_dir  в локальную папку lxmlpath
    def gz_get_ftp_files(self, aftp, aftp_dir, axmlpath, afields, adoc, adfrom, adto, aregions=None):
        # создаем DataFrame для данных контрактов
        docs = pd.DataFrame(columns=afields.split(','))
        for s in self.get_ftp_dir_list(aftp, "/out/published/"):
            i = 0
            # s=get_ftp_dir_list(ftp, "/out/published/")[0]
            if aregions == None or ((aregions != None) and (s in aregions)):
                try:
                    lpath = "/out/published/" + s + aftp_dir
                    time.asctime(time.localtime(time.time()))
                    self.log('Каталог: ' + s)
                    aftp.cwd(lpath)
                    lftplist = self.get_ftp_dir_list(aftp, lpath)
                    self.log('Всего файлов (архивов) в FTP каталоге: ' + str(len(lftplist)))
                    for f in lftplist:
                        if (f[-4:] == '.zip') and self.__compdate(f, adfrom, adto):
                            self.log(f)
                            self.download(aftp, lpath, axmlpath, f)
                            i += 1
                            archlist = self.unzipall(axmlpath, f)
                            # Поскольку в разных архивах содержатся XML файлы с одинаковыми названиями
                            # - добавляем к названию файла уникальный для архива номер, либо после парсинга сразу удаляем файл XML
                            for x in archlist:
                                # если файл не нулевой длины - парсим xml и загружаем данные в DataFrame
                                if os.path.getsize(axmlpath + x) != 0:
                                    self.log(x)
                                    doc = self.parse_xml(axmlpath + x, adoc)
                                    # Удаляем дубли в списке оквэд
                                    # добавляем имена файлов zip и xml в список
                                    doc.append(len(doc[3]))
                                    doc.append(f)
                                    doc.append(x)
                                    doc.append(s)
                                    docs.loc[len(docs)] = doc
                                os.remove(axmlpath + x)
                                # удаляем файл архива
                            os.remove(axmlpath + f)
                            if i % 100 == 0:
                                self.log(' | Обработано ' + str(i) + ' файлов из ' + str(len(lftplist))
                                         + ' (' + str(int(round(float(i) / len(lftplist) * 100))) + '%)')
                except ftplib.error_perm:
                    self.log('Ошибка! В папке ' + s + ' каталог ' + aftp_dir + ' не существует!')
        # Могут досылать исправленные данные по контракту в разные дни. Это надо обрабатывать.
        docs = docs.drop_duplicates(['nnumber'], keep='last')
        docs.index = range(len(docs.index))
        self.log(u'Окончание обработки:')
        return docs

    def getreglist(self, adoc, afz):
        llist = self.__config.get(adoc, 'regions').split(',')
        if llist == '*':
            lftp = self.ftpconnect(afz)
            return self.get_ftp_dir_list(lftp, self.__config.get(adoc, 'commonpath'))
        else:
            return llist

    # Блок подготовки и проверки SQL команды
    def insert_prepare(self, asql, acursor):
        try:
            acursor.prepare(asql)
        except cx_Oracle.DatabaseError as e:
            self.log('Ошибка! Не возможно приготовить курсор (' + str(e.message) + ')')
            self.log(asql)
            exit(1)


    def oraconnect(self, aconstr):
        self.conn = cx_Oracle.connect(aconstr)
        self.cursor = self.conn.cursor()

    def oradisc(self):
        self.cursor.close()
        self.conn.close()

    def load_from_ora(self, asql):
        try:
            if self.cursor.statement != asql:
                self.insert_prepare(asql, self.cursor)
            self.cursor.execute(self.cursor.statement)
            ltable = self.cursor.fetchall()
            # получаем названия полей запроса
            lcols = [col_desc[0] for col_desc in self.cursor.description]
        except cx_Oracle.DatabaseError as e:
            self.log('Ошибка! Не возможно выгрузить данные (' + str(e.message) + ')')
            self.log(self.cursor.statement)
            self.oradisc()
            exit(1)
        df = pd.DataFrame(ltable, columns=lcols)
        return df

    def save_to_ora(self, asql, abindvar):
        try:
            if self.cursor.statement != asql:
                self.insert_prepare(asql, self.cursor)
            self.cursor.execute(self.cursor.statement, abindvar)
            ltable = self.cursor.fetchall()
        except cx_Oracle.DatabaseError as e:
            self.log('Ошибка! Не возможно вставить строку (' + str(e.message) + ')')
            self.log(self.cursor.statement)
            self.log(', '.join([str(s) for s in abindvar.values()]))
            self.oradisc()
            exit(1)

    # def load_df_to_ora(adf):
    #     conn = cx_Oracle.connect('analytics/analytics@fst_rac')
    #     cursor = conn.cursor()
    #     try:
    #         isql = """
    #                   INSERT INTO ZAKUPKI_PROTOKOLVK
    #                   (nnumber, mcode, mname, nsupplier, ncount, zip, nxml, nregion)
    #                   VALUES (:s_nnumber, :s_mcode, :s_mname, :s_nsupplier, :s_ncount, :s_zip, :s_nxml, :s_nregion)
    #                """
    #         # Блок подготовки и проверки SQL команды
    #         for i in range(len(adf.index)):
    #             # if adf['NOTICENUM'][i]=='NULL':
    #             insert_prepare(isql, cursor)
    #             # else:
    #             #    insert_prepare(usql,cursor)
    #             try:
    #                 # Выполнение Insert
    #                 cursor.execute(cursor.statement, s_nnumber=adf['nnumber'][i],
    #                                s_mcode=adf['mcode'][i], s_mname=adf['mname'][i],
    #                                s_nsupplier=None, s_ncount=adf['ncount'][i],
    #                                s_zip=adf['zip'][i], s_nxml=adf['xml'][i], s_nregion=adf['region'][i]
    #                                )
    #             except cx_Oracle.DatabaseError as exception:
    #                 print('Failed to insert row')
    #                 print(exception)
    #                 print(cursor.statement)
    #                 # print str(df['NYEAR'][i])+'|'+str(df['GROUP_NUMBER'][i])+'|'+str(df['GROUP_NAME'][i])+'|'+str(df['ORG_REG_NUMBER'][i])+'|'+str(df['ORG_NAME'][i])
    #                 exit(1)
    #             if (i > 0) and (i % 100 == 0):
    #                 print(u' Обработано строк: ' + str(i))
    #         conn.commit()
    #     finally:
    #         cursor.close()
    #         conn.close()

    def loadxmltoora(self):
        lfz = 'docs223'
        lftpfz = 'ftp223'
        lsql = """
                   INSERT INTO ZAKUPKI_PROTOKOL 
                   (nnumber, mcode, mname, nsupplier, ncount, zip, nxml, nregion) 
                   VALUES (:s_nnumber, :s_mcode, :s_mname, :s_nsupplier, :s_ncount, :s_zip, :s_nxml, :s_nregion)
               """
        self.log('***Старт загрузки***')
        try:
            self.oraconnect('zakupki/dD9qHxQD3t5w@FST_RAC')
            for lval in self.__config.items(lfz):
                ldoc = lfz + '.' + lval[0]
                lreglist = self.getreglist(ldoc, lftpfz)
                for reg in lreglist:
                    lftp = self.ftpconnect(lftpfz)
                    ldocs = self.gz_get_ftp_files(lftp, self.__config.get(ldoc, 'ftppath'),
                                                  self.__config.get('common', 'xmldir'),
                                                  self.__config.get(ldoc, 'fields'),
                                                  lval[0], self.__config.get(ldoc, 'datefrom'),
                                                  self.__config.get(ldoc, 'dateto'), lreglist)
                    # load_df_to_ora(contracts)
                    for i in range(len(ldocs.index)):
                        lbindvar=dict(
                            s_nnumber=ldocs['nnumber'][i],
                            s_mcode=ldocs['mcode'][i],
                            s_mname=ldocs['mname'][i],
                            s_nsupplier=None,
                            s_ncount=ldocs['ncount'][i],
                            s_zip=ldocs['zip'][i],
                            s_nxml=ldocs['xml'][i],
                            s_nregion=ldocs['region'][i]
                        )
                        #self.save_to_ora(lsql, lbindvar)
            return ldocs
        except configparser.NoOptionError as eo:
            self.log('Ошибка! Некорректный конфигурационный файл settings.ini (' + str(eo.message) + ')')
            exit(1)
        except Exception as e:
            self.log('Ошибка! (' + str(e) +')')
            exit(1)
        finally:
            self.oradisc()
            self.log('***Окончание загрузки***')