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
        self.conn = None
        self.cursor = None
        self.__maxdate = None

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

    def __saveconfig(self, asection, aoption, avalue):
        try:
            self.__config.set(asection, aoption, avalue)
            # Вносим изменения в конфиг. файл.
            with open('settings.ini', "w") as cfile:
                self.__config.write(cfile)
        except Exception as e:
            self.log('Ошибка! (' + str(e) + ')')

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

    # функция обработки одноуровневых составных эелементов
    def __xpath_composite_one_level(self, aelements):
        llist = list()
        for index, value in enumerate(aelements):
            if value.text.strip() == '':
                llist.append(dict())
            else:
                llist[len(llist) - 1][value.tag[value.tag.find('}') + 1:]] = value.text.strip()
        return llist

    # функция обработки двухуровневых составных эелементов
    def __xpath_composite_two_level(self, aparentelements, achildelements):
        lparlist = self.__xpath_composite_one_level(aparentelements)
        llist = list()
        nc = -1
        for index, value in enumerate(achildelements):
            if index < len(achildelements) - 1 and value.text.strip() == '':
                if achildelements[index + 1].text.strip() == '':
                    nc += 1
                else:
                    llist.append(lparlist[nc].copy())
            else:
                llist[len(llist) - 1][value.tag[value.tag.find('}') + 1:]] = value.text.strip()
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
                self.__xpath_nulls(root.xpath('.//ns2:guid', namespaces=lnamespaces)),
                self.__xpath_nulls(root.xpath('.//ns2:urlEIS', namespaces=lnamespaces)),
                self.__xpath_nulls(root.xpath('.//ns2:registrationNumber', namespaces=lnamespaces)),
                root.xpath('.//ns2:purchaseInfo/ns:purchaseNoticeNumber', namespaces=lnamespaces)[0].text,
                root.xpath('.//ns2:purchaseInfo/ns:purchaseMethodCode', namespaces=lnamespaces)[0].text,
                root.xpath('.//ns2:purchaseInfo/ns:purchaseCodeName', namespaces=lnamespaces)[0].text,
                self.__xpath_composite_two_level(
                    # __xpath_list(
                    root.xpath(
                        './/ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:lot | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:lot/ns2:ordinalNumber | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:lot/ns2:guid',
                        namespaces=lnamespaces
                    ),
                    root.xpath(
                        #ns2:protocolLotApplications[ns2:application] - ищет все тэги protocolLotApplications,
                        # для которых существует хотя бы один тэг application (нужно, чтобы отсеять,
                        # когда не подана ни одна заявка)
                        './/ns2:lotApplicationsList/ns2:protocolLotApplications[ns2:application]/ns2:lot | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:name | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:inn | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:kpp | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:ogrn | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:supplierInfo/ns:address | \
                         .//ns2:lotApplicationsList/ns2:protocolLotApplications/ns2:application/ns2:winnerIndication',
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
            if self.__maxdate == None \
                    or self.__maxdate < dt.datetime.strptime(ldfile, '%Y%m%d'):
                self.__maxdate = dt.datetime.strptime(ldfile, '%Y%m%d')
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
                            self.download(aftp, lpath, axmlpath, f)
                            i += 1
                            archlist = self.unzipall(axmlpath, f)
                            # Поскольку в разных архивах содержатся XML файлы с одинаковыми названиями
                            # - добавляем к названию файла уникальный для архива номер, либо после парсинга сразу удаляем файл XML
                            for x in archlist:
                                # если файл не нулевой длины - парсим xml и загружаем данные в DataFrame
                                if os.path.getsize(axmlpath + x) != 0:
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
                                self.log(' | Обработано ' + str(i) + ' файлов архивов из ' + str(len(lftplist))
                                         + ' (' + str(int(round(float(i) / len(lftplist) * 100))) + '%)')
                except ftplib.error_perm:
                    self.log('Ошибка! В папке ' + s + ' каталог ' + aftp_dir + ' не существует!')
        # Могут досылать исправленные данные по контракту в разные дни. Это надо обрабатывать.
        docs = docs.drop_duplicates(['nnumber'], keep='last')
        docs.index = range(len(docs.index))
        self.log(u'Окончание обработки.')
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
            self.log('Ошибка! Не возможно приготовить курсор (' + str(e) + ')')
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
            self.log('Ошибка! Не возможно выгрузить данные (' + str(e) + ')')
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
            self.conn.commit()
        except cx_Oracle.DatabaseError as e:
            self.log('Ошибка! Не возможно вставить строку (' + str(e) + ')')
            self.log(self.cursor.statement)
            self.log(', '.join([str(s) for s in abindvar.values()]))
            self.oradisc()
            exit(1)

    def savedoctoora(self,adocs, adoctype):
        if adoctype == 'purchaseprotocol':
            lsql1 = """
                   MERGE INTO PROTOCOLS p
                   USING (SELECT :s_doctype as doctype, :s_urleis as urleis, :s_guid as guid, :s_regnumber as regnumber, 
                                 :s_nnumber as nnumber, :s_mcode as mcode, :s_mname as mname, :s_ncount as ncount, 
                                 :s_zip as zip, :s_nxml as nxml, :s_nregion as nregion FROM DUAL) v
                   ON (p.nnumber=v.nnumber)
                   WHEN MATCHED THEN
                      UPDATE SET p.doctype = v.doctype, p.urleis = v.urleis, p.guid = v.guid, p.regnumber=v.regnumber, 
                                 p.mcode = v.mcode, p.mname = v.mname, p.ncount = v.ncount, p.zip = v.zip,
                                 p.nxml = v.nxml, p.nregion = v.nregion
                   WHEN NOT MATCHED THEN
                      INSERT (p.doctype, p.urleis, p.guid, p.regnumber, p.nnumber, p.mcode, p.mname, p.ncount, p.zip, p.nxml, p.nregion)
                      VALUES (v.doctype, v.urleis, v.guid, v.regnumber, v.nnumber, v.mcode, v.mname, v.ncount, v.zip, v.nxml, v.nregion)
                   """
            lsql2 = """
                   MERGE INTO SUPPLIERS p
                   USING (SELECT :name as name, :inn as inn, :kpp as kpp, :ogrn as ogrn, :address as address, 
                                 :winnerIndication as winnerind, :ordinalNumber as lotnumber, :guid as lotguid,
                                 :protguid as protguid FROM DUAL) v
                   ON (p.protguid=v.protguid and p.lotguid=v.lotguid and p.ogrn=v.ogrn)
                   WHEN MATCHED THEN
                      UPDATE SET p.name = v.name, p.inn = v.inn, p.kpp = v.kpp, p.address = v.address, 
                                 p.winnerind = v.winnerind, p.lotnumber = v.lotnumber
                   WHEN NOT MATCHED THEN
                      INSERT (p.name, p.inn, p.kpp, p.ogrn, p.address, p.winnerind, p.lotnumber, p.lotguid, p.protguid)
                      VALUES (v.name, v.inn, v.kpp, v.ogrn, v.address, v.winnerind, v.lotnumber, v.lotguid, v.protguid)
                   """
            for i in range(len(adocs.index)):
                lbindvar = dict(
                    s_doctype=2,
                    s_guid=adocs['guid'][i],
                    s_urleis=adocs['urleis'][i],
                    s_regnumber=adocs['regnumber'][i],
                    s_nnumber=adocs['nnumber'][i],
                    s_mcode=adocs['mcode'][i],
                    s_mname=adocs['mname'][i],
                    s_ncount=adocs['ncount'][i],
                    s_zip=adocs['zip'][i],
                    s_nxml=adocs['xml'][i],
                    s_nregion=adocs['region'][i]
                )
                self.save_to_ora(lsql1, lbindvar)
                lsd = adocs['nsupplier'][i]
                for sd in lsd:
                    lbindvar = dict(
                        name = sd.get('name',''),
                        inn = sd.get('inn',''),
                        kpp = sd.get('kpp',''),
                        ogrn = sd.get('ogrn',''),
                        address = sd.get('address',''),
                        winnerIndication = sd.get('winnerIndication',''),
                        ordinalNumber = sd.get('ordinalNumber',''),
                        guid = sd.get('guid',''),
                        protguid = adocs['guid'][i]
                    )
                    self.save_to_ora(lsql2, lbindvar)
                if (i > 0) and (i % 500 == 0):
                    self.log(u' Обработано строк: ' + str(i))

    def loadxmltoora(self):
        lfz = 'docs223'
        lftpfz = 'ftp223'
        self.log('***Старт загрузки***')
        try:
            for lval in self.__config.items(lfz):
                ldoc = lfz + '.' + lval[0]
                lreglist = self.getreglist(ldoc, lftpfz)
                self.oraconnect('zakupki/dD9qHxQD3t5w@FST_RAC')
                for reg in lreglist:
                    lftp = self.ftpconnect(lftpfz)
                    ldocs = self.gz_get_ftp_files(lftp, self.__config.get(ldoc, 'ftppath'),
                                                  self.__config.get('common', 'xmldir'),
                                                  self.__config.get(ldoc, 'fields'),
                                                  lval[0], self.__config.get(ldoc, 'datefrom'),
                                                  self.__config.get(ldoc, 'dateto'), lreglist)
                    self.savedoctoora(ldocs,lval[0])
                    self.log('Документы %s для региона %s загружены в базу за период с %s по %s .'
                             % (ldoc, reg, self.__config.get(ldoc, 'datefrom'), "{:%Y%m%d}".format(self.__maxdate)))
            if self.__config.get(ldoc, 'dateto') == '*':
                self.__saveconfig(ldoc,'datefrom',"{:%Y%m%d}".format(self.__maxdate))
                self.__saveconfig(ldoc, 'dateto', '*')
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