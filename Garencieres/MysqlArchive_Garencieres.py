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
        config.read('/data/mysql/scripts/MysqlArchiveScripts/Config/Garencieres.cfg')
        self.ArchiveDb = config.get('Garencieres', 'AECHIVE_DB')
        self.OrigHost = config.get('Garencieres', 'ORIG_HOST')
        self.OrigUser = config.get('Garencieres', 'ORIG_USER')
        self.OrigPasswd = config.get('Garencieres', 'ORIG_PASSWD')
        self.OrigDb = config.get('Garencieres', 'ORIG_DB')
        self.OrigPort = config.getint('Garencieres', 'ORIG_PORT')
        self.SourceHost = config.get('Garencieres', 'SOURCE_HOST')
        self.SourceUser = config.get('Garencieres', 'SOURCE_USER')
        self.SourcePasswd = config.get('Garencieres', 'SOURCE_PASSWD')
        self.SourceDb = config.get('Garencieres', 'SOURCE_DB')
        self.SourcePort = config.getint('Garencieres', 'SOURCE_PORT')
        self.TargetHost = config.get('Garencieres', 'TARGET_HOST')
        self.TargetUser = config.get('Garencieres', 'TARGET_USER')
        self.TargetPasswd = config.get('Garencieres', 'TARGET_PASSWD')
        self.TargetDb = config.get('Garencieres', 'TARGET_DB')
        self.TargetPort = config.getint('Garencieres', 'TARGET_PORT')

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

    def CreateStockinCode(self):
        SqlCreateStockinCode = "INSERT IGNORE INTO archive_stockin_code_mapping \
                               (stockin_code, archive_date) \
                               SELECT code, '%s' FROM \
                               garencieres.oper_stockin a WHERE EXISTS( \
                               SELECT 1 FROM archive_order_id_mapping b \
                               WHERE a.order_id = b.order_id \
                               AND b.archive_date <= \"%s\")"
        SqlCreateStockinCode = SqlCreateStockinCode % (self.Today, self.Today)
        self.ExecSql(SqlCreateStockinCode, self.SourceHost, self.SourceUser,
                     self.SourcePasswd, self.SourceDb, self.SourcePort)

    def CreatePickingId(self):
        SqlCreatePickingId = "INSERT IGNORE INTO archive_picking_id_mapping \
                             (picking_id, archive_date) \
                             SELECT picking_id, '%s' FROM \
                             garencieres.oper_picking_stockin a WHERE EXISTS( \
                             SELECT 1 FROM archive_stockin_code_mapping b \
                             WHERE a.stockin_code = b.stockin_code \
                             AND b.archive_date <= \"%s\")"
        SqlCreatePickingId = SqlCreatePickingId % (self.Today, self.Today)
        self.ExecSql(SqlCreatePickingId, self.SourceHost, self.SourceUser,
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
               table_name not in ('user_order_shipment') and \
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

    def ArchiveData_OrderId(self):
        TableList = []
        Con = "exists(select 1 from %s.%s where order_id = %s.order_id \
               and archive_date <= \"%s\")"
        MapTb = "archive_order_id_mapping"

        TableTuple = self.GetArchiveTbList("order_id")
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

    def ArchiveData_StockinCode(self):
        TableList = []
        Con = "exists(select 1 from %s.%s where stockin_code = %s.stockin_code \
               and archive_date <= \"%s\")"
        MapTb = "archive_stockin_code_mapping"

        TableTuple = self.GetArchiveTbList("stockin_code")
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

    def ArchiveData_PickingId(self):
        TableList = []
        Con = "exists(select 1 from %s.%s where picking_id = %s.picking_id \
               and archive_date <= \"%s\")"
        MapTb = "archive_picking_id_mapping"

        TableTuple = self.GetArchiveTbList("picking_id")
        for TableName in TableTuple:
            if TableName[0] != "oper_picking_stockin":
                if TableName[0] != "oper_picker_picking":
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
    等待order_id生成完毕
    """
    WaitReturn = MysqlArchive().WaitForOrderId()

    if WaitReturn is True:
        """
        将order_id从userorder_xx库,同步到目标数据库
        """
        MysqlArchive().CopyOrderId()
        """
        根据order_id生成stockin_code
        """
        MysqlArchive().CreateStockinCode()
        """
        根据stockin_code生成picking_id
        """
        MysqlArchive().CreatePickingId()
        """
        根据order_id归档指定数据库的数据
        """
        MysqlArchive().ArchiveData_OrderId()
        """
        根据stockin_code归档指定数据库的数据
        """
        MysqlArchive().ArchiveData_StockinCode()
        """
        根据picking_id归档指定数据库的数据
        """
        MysqlArchive().ArchiveData_PickingId()
