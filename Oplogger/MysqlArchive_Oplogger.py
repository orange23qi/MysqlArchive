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
        config.read('/data/mysql/scripts/MysqlArchiveScripts/Config/Oplogger.cfg')
        self.ArchiveDb = config.get('Oplogger', 'AECHIVE_DB')
        self.SourceHost = config.get('Oplogger', 'SOURCE_HOST')
        self.SourceUser = config.get('Oplogger', 'SOURCE_USER')
        self.SourcePasswd = config.get('Oplogger', 'SOURCE_PASSWD')
        self.SourceDb = config.get('Oplogger', 'SOURCE_DB')
        self.SourcePort = config.getint('Oplogger', 'SOURCE_PORT')
        self.TargetHost = config.get('Oplogger', 'TARGET_HOST')
        self.TargetUser = config.get('Oplogger', 'TARGET_USER')
        self.TargetPasswd = config.get('Oplogger', 'TARGET_PASSWD')
        self.TargetDb = config.get('Oplogger', 'TARGET_DB')
        self.TargetPort = config.getint('Oplogger', 'TARGET_PORT')

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
        # print ArchiverCommand
        Output = commands.getoutput(ArchiverCommand)

        ErrorType1 = "The following columns exist in --source "
        ErrorType1 += "but not --dest: is_active"
        if Output == ErrorType1:
            Msg = "\nError : Archive error. Please check table structure "
            Msg = Msg + "between archive db and pro. "
            Msg = "Db : " + SDb + "\nTableName : " + STb + Msg
            self.SendMsg(Msg, '18616687370')

    def ArchiveData_OperateLog(self):
        Con = "operate_date < UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 2 MONTH))"

        TableName = 'dic_operate_log'
        self.PtArchiver(self.SourceHost, self.SourcePort, self.SourceUser,
                        self.SourcePasswd, self.ArchiveDb, TableName,
                        self.TargetHost, self.TargetPort, self.TargetUser,
                        self.TargetPasswd, self.TargetDb, TableName,
                        Con, 1)

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
    归档operate_date是两个月前的数据
    """
    MysqlArchive().ArchiveData_OperateLog()
