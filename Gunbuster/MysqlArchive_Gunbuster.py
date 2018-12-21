# -*-coding: utf-8-*-

import ConfigParser
import pymysql
import datetime
import time
import commands
import requests
import json


class MysqlArchive(object):
    def __init__(self):
        Now = datetime.datetime.now()
        self.TodayMin = datetime.datetime.combine(Now, datetime.time.min)
        self.UnixTodayMin = time.mktime(self.TodayMin.timetuple())
        self.Today = datetime.datetime.now().date().strftime("%Y-%m-%d")

        config = ConfigParser.RawConfigParser()
        config.read('/data/mysql/scripts/MysqlArchiveScripts/Config/Gunbuster.cfg')
        self.ArchiveDb = config.get('Gunbuster', 'AECHIVE_DB')
        self.SourceHost = config.get('Gunbuster', 'SOURCE_HOST')
        self.SourceUser = config.get('Gunbuster', 'SOURCE_USER')
        self.SourcePasswd = config.get('Gunbuster', 'SOURCE_PASSWD')
        self.SourceDb = config.get('Gunbuster', 'SOURCE_DB')
        self.SourcePort = config.getint('Gunbuster', 'SOURCE_PORT')
        self.TargetHost = config.get('Gunbuster', 'TARGET_HOST')
        self.TargetUser = config.get('Gunbuster', 'TARGET_USER')
        self.TargetPasswd = config.get('Gunbuster', 'TARGET_PASSWD')
        self.TargetDb = config.get('Gunbuster', 'TARGET_DB')
        self.TargetPort = config.getint('Gunbuster', 'TARGET_PORT')

    def ExecSql(self, Sql, Host, User, Passwd, Db, Port):
        db = pymysql.connect(host=Host,
                             user=User,
                             passwd=Passwd,
                             db=Db,
                             port=Port)
        cursor = db.cursor()

        cursor.execute(Sql)
        db.commit()
        db.close()

    def PtArchiver(self, SHost, SPort, SUser, SPasswd, SDb, STb, THost,
                   TPort, TUser, TPasswd, TDb, TTb, Condition, IsDel):
        DelCommand = "--no-delete"
        if IsDel == 1:
            DelCommand = ""

        ArchiverCommand = "pt-archiver --source h=%s,P=%d,u=%s,p=%s,D=%s,t=%s \
                           --dest h=%s,P=%d,u=%s,p=%s,D=%s,t=%s,L=yes \
                           --charset=UTF8 --progress 10000 --limit=10000 \
                           --txn-size 10000 --bulk-insert --bulk-delete \
                           --where '%s' --quiet --no-check-charset --ignore \
                           --nosafe-auto-increment %s"
        ArchiverCommand = ArchiverCommand % (SHost, SPort, SUser, SPasswd, SDb,
                                             STb, THost, TPort, TUser, TPasswd,
                                             TDb, TTb, Condition, DelCommand)
        #print ArchiverCommand
        Output = commands.getoutput(ArchiverCommand)

        ErrorType1 = "The following columns exist in --source "
        ErrorType1 += "but not --dest: is_active"
        if Output == ErrorType1:
            Msg = "\nError : Archive error. Please check table structure "
            Msg = Msg + "between archive db and pro. "
            Msg = "Db : " + SDb + "\nTableName : " + STb + Msg
            self.SendMsg(Msg, '18616687370')

    def GetArchiveTbList(self, ColumName):
        Sql = "select table_name from information_schema.columns where \
               column_name = '%s' and table_schema = '%s';"
        Sql = Sql % (ColumName, self.ArchiveDb)

        db = pymysql.connect(host=self.SourceHost,
                             user=self.SourceUser,
                             passwd=self.SourcePasswd,
                             db=self.SourceDb,
                             port=self.SourcePort)
        cursor = db.cursor()
        cursor.execute(Sql)
        TableList = cursor.fetchall()
        db.close()

        return TableList

    def ArchiveData_OperPurchaseOrderId(self):
        TableList = []
        Con = "exists(select 1 from %s.%s where oper_purchase_order_id = %s.oper_purchase_order_id \
               and archive_date <= \"%s\")"
        MapTb = "archive_oper_purchase_order_id_mapping"

        TableTuple = self.GetArchiveTbList("oper_purchase_order_id")
        for TableName in TableTuple:
            TableList.append(TableName[0])

        for TableName in TableList:
            ConEach = Con % (self.SourceDb, MapTb, TableName, self.Today)

            print TableName
            self.PtArchiver(self.SourceHost, self.SourcePort, self.SourceUser,
                            self.SourcePasswd, self.ArchiveDb, TableName,
                            self.TargetHost, self.TargetPort, self.TargetUser,
                            self.TargetPasswd, self.TargetDb, TableName,
                            ConEach, 1)

    def SendMsg(self, Info, User):
        Url = "http://wechat.65dg.me/robot/send?"
        Url += "access_token=dad5d1b5465f645959ef107aa544b215"
        Users = User.split(",")
        Msg = {
                "msgtype": "text",
                "text": {
                            "content": "{}".format(Info)
                        },
                "at": {
                        "atMobiles": Users,
                      }
              }

        Header = {
                     'Content-Type': 'application/json'
                 }
        requests.post(url=Url, data=json.dumps(Msg), headers=Header)


if __name__ == '__main__':
    """
    根据oper_purchase_order_id归档指定数据库的数据
    """
    MysqlArchive().ArchiveData_OperPurchaseOrderId()
