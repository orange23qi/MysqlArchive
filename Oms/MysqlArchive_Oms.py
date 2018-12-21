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
        config.read('/data/mysql/scripts/MysqlArchiveScripts/Config/Oms.cfg')
        self.OrigHost = config.get('Oms', 'ORIG_HOST')
        self.OrigUser = config.get('Oms', 'ORIG_USER')
        self.OrigPasswd = config.get('Oms', 'ORIG_PASSWD')
        self.OrigDb = config.get('Oms', 'ORIG_DB')
        self.OrigPort = config.getint('Oms', 'ORIG_PORT')
        self.SourceHost = config.get('Oms', 'SOURCE_HOST')
        self.SourceUser = config.get('Oms', 'SOURCE_USER')
        self.SourcePasswd = config.get('Oms', 'SOURCE_PASSWD')
        self.SourceDb = config.get('Oms', 'SOURCE_DB')
        self.SourcePort = config.getint('Oms', 'SOURCE_PORT')
        self.TargetHost = config.get('Oms', 'TARGET_HOST')
        self.TargetUser = config.get('Oms', 'TARGET_USER')
        self.TargetPasswd = config.get('Oms', 'TARGET_PASSWD')
        self.TargetPort = config.getint('Oms', 'TARGET_PORT')

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

    def WaitForOrderId(self):
        Sql = "SELECT COUNT(1) FROM eznearline_source.order_id_complete_log \
               WHERE complete_date = '%s';"
        Sql = Sql % (self.Today)

        Status = 0
        Count = 0
        while Status == 0:
            db = pymysql.connect(host=self.OrigHost,
                                 user=self.OrigUser,
                                 passwd=self.OrigPasswd,
                                 port=self.OrigPort)
            cursor = db.cursor()
            cursor.execute(Sql)
            DbResult = cursor.fetchone()
            db.close()

            Status = DbResult[0]
            if Status == 0:
                time.sleep(600)

            Count += 1
            if Count >= 60:
                return False
        return True

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
        if Output != "":
            if Output == ErrorType1:
                Msg = "\nError : Archive error. Please check table structure "
                Msg = Msg + "between archive db and pro. "
                Msg = "Db : " + SDb + "\nTableName : " + STb + Msg
            else:
                Msg = "\nError : Archive Failed! \n" + Output
                Msg = "Db : " + SDb + "\nTableName : " + STb + Msg
            self.SendMsg(Msg, '18616687370')

    def CopyOrderId(self):
        SourceTable = "archive_order_id_mapping"
        TargetTable = "archive_order_id_mapping"
        SqlCondition = "archive_date <= \"%s\""
        SqlCondition = SqlCondition % (self.Today)

        self.PtArchiver(self.OrigHost, self.OrigPort, self.OrigUser,
                        self.OrigPasswd, self.OrigDb, SourceTable,
                        self.SourceHost, self.SourcePort, self.SourceUser,
                        self.SourcePasswd, self.SourceDb, TargetTable,
                        SqlCondition, 0)

    def GetArchiveTbList(self, ColumName, SchemaName):
        Sql = "select table_name from information_schema.columns where \
               column_name = '%s' and table_schema = '%s';"
        Sql = Sql % (ColumName, SchemaName)

        db = pymysql.connect(host=self.OrigHost,
                             user=self.OrigUser,
                             passwd=self.OrigPasswd,
                             db=self.OrigDb,
                             port=self.OrigPort)
        cursor = db.cursor()
        cursor.execute(Sql)
        TableList = cursor.fetchall()
        db.close()

        return TableList

    def ArchiveData_OrderId(self, Country):
        TableList = []
        SchemaName = "oms_" + Country
        TargetDb = "eznearline_" + SchemaName
        Con = "exists(select 1 from %s.%s where order_id = %s.%s \
               and archive_date <= \"%s\" and country = \"%s\")"
        MapTb = "archive_order_id_mapping"

        TableTuple = self.GetArchiveTbList("order_id", SchemaName)
        for TableName in TableTuple:
            TableList.append(TableName[0])

        TableList.append('user_order')

        for TableName in TableList:
            ColumName = 'order_id'
            if TableName == 'user_order':
                ColumName = 'id'

            ConEach = Con % (self.OrigDb, MapTb, TableName,
                             ColumName, self.Today, Country)

            print TableName
            self.PtArchiver(self.OrigHost, self.OrigPort, self.OrigUser,
                            self.OrigPasswd, SchemaName, TableName,
                            self.TargetHost, self.TargetPort, self.TargetUser,
                            self.TargetPasswd, TargetDb, TableName,
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
    等待order_id生成完毕
    """
    WaitRetrun = MysqlArchive().WaitForOrderId()
    MysqlArchive().CopyOrderId()
    """
    根据order_id归档指定数据的库数据
    """
    if WaitRetrun is True:
        for Country in ('id', 'th', 'my', 'sg', 'pk', 'twc'):
            print Country
            MysqlArchive().ArchiveData_OrderId(Country)
