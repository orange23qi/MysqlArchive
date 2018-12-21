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
        config.read('/data/mysql/scripts/MysqlArchiveScripts/Config/Eltreum.cfg')
        self.ArchiveDb = config.get('Eltreum', 'AECHIVE_DB')
        self.OrigHost = config.get('Eltreum', 'ORIG_HOST')
        self.OrigUser = config.get('Eltreum', 'ORIG_USER')
        self.OrigPasswd = config.get('Eltreum', 'ORIG_PASSWD')
        self.OrigDb = config.get('Eltreum', 'ORIG_DB')
        self.OrigPort = config.getint('Eltreum', 'ORIG_PORT')
        self.SourceHost = config.get('Eltreum', 'SOURCE_HOST')
        self.SourceUser = config.get('Eltreum', 'SOURCE_USER')
        self.SourcePasswd = config.get('Eltreum', 'SOURCE_PASSWD')
        self.SourceDb = config.get('Eltreum', 'SOURCE_DB')
        self.SourcePort = config.getint('Eltreum', 'SOURCE_PORT')
        self.TargetHost = config.get('Eltreum', 'TARGET_HOST')
        self.TargetUser = config.get('Eltreum', 'TARGET_USER')
        self.TargetPasswd = config.get('Eltreum', 'TARGET_PASSWD')
        self.TargetDb = config.get('Eltreum', 'TARGET_DB')
        self.TargetPort = config.getint('Eltreum', 'TARGET_PORT')

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
            if Count > 60:
                return False
        return True

    def CreateOperPurchaseOrderId(self):
        SqlCreateOperPurchaseOrderId = "INSERT IGNORE INTO archive_oper_purchase_order_id_mapping\
                                       (oper_purchase_order_id, archive_date) \
                                       SELECT a.id, '%s' FROM \
                                       eltreum.oper_purchase_order a WHERE EXISTS( \
                                       SELECT 1 FROM archive_order_id_mapping b \
                                       WHERE a.order_id_mysql = b.order_id \
                                       AND b.archive_date <= \"%s\")"
        SqlCreateOperPurchaseOrderId = SqlCreateOperPurchaseOrderId % (self.Today, self.Today)
        self.ExecSql(SqlCreateOperPurchaseOrderId, self.SourceHost, self.SourceUser,
                     self.SourcePasswd, self.SourceDb, self.SourcePort)

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

    def ArchiveData_OperSyncOrderItemMssql(self):
        Con = "exists(select 1 from %s.%s where order_id = %s.new_order_id \
               and archive_date <= \"%s\")"
        MapTb = "archive_order_id_mapping"
        TableName = "oper_sync_order_item_mssql"

        ConEach = Con % (self.SourceDb, MapTb, TableName, self.Today)

        print TableName
        self.PtArchiver(self.SourceHost, self.SourcePort, self.SourceUser,
                        self.SourcePasswd, self.ArchiveDb, TableName,
                        self.TargetHost, self.TargetPort, self.TargetUser,
                        self.TargetPasswd, self.TargetDb, TableName,
                        ConEach, 1)

    def ArchiveData_OperSyncOrderItemMysql(self):
        Con = "exists(select 1 from %s.%s where order_id = %s.order_id \
               and archive_date <= \"%s\")"
        MapTb = "archive_order_id_mapping"
        TableName = "oper_sync_order_item_mysql"

        ConEach = Con % (self.SourceDb, MapTb, TableName, self.Today)

        print TableName
        self.PtArchiver(self.SourceHost, self.SourcePort, self.SourceUser,
                        self.SourcePasswd, self.ArchiveDb, TableName,
                        self.TargetHost, self.TargetPort, self.TargetUser,
                        self.TargetPasswd, self.TargetDb, TableName,
                        ConEach, 1)

    def ArchiveData_OperPurchaseOrder(self):
        Con = "exists(select 1 from %s.%s where order_id = %s.order_id_mysql \
               and archive_date <= \"%s\")"
        MapTb = "archive_order_id_mapping"
        TableName = "oper_purchase_order"

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
    等待order_id生成完毕
    """
    WaitReturn = MysqlArchive().WaitForOrderId()

    if WaitReturn is True:
        """
        将order_id从userorder_xx库,同步到目标数据库
        """
        MysqlArchive().CopyOrderId()
        """
        根据order_id生成oper_purchase_order_id
        """
        MysqlArchive().CreateOperPurchaseOrderId()
        """
        根据oper_purchase_order_id归档指定数据库的数据
        """
        MysqlArchive().ArchiveData_OperPurchaseOrderId()
        """
        根据order_id归档oper_sync_order_item_mssql表数据
        """
        MysqlArchive().ArchiveData_OperSyncOrderItemMssql()
        """
        根据order_id归档oper_sync_order_item_mysql表数据
        """
        MysqlArchive().ArchiveData_OperSyncOrderItemMysql()
        """
        根据order_id归档oper_purchase_order表数据
        """
        MysqlArchive().ArchiveData_OperPurchaseOrder()
