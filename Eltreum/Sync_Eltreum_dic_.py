# -*-coding: utf-8-*-
import ConfigParser
import pymysql
import os

class SyncData(object):
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('/data/mysql/scripts/MysqlArchiveScripts/Config/Eltreum.cfg')
        self.ArchiveDb = config.get('Eltreum', 'AECHIVE_DB')
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

    def GetTableList(self):
        Sql = "select table_name from information_schema.tables where \
               table_name like 'dic_%%' and table_schema = '%s';"

        Sql = Sql % (self.ArchiveDb)

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

    def GetDump(self):
        MysqlCommand = "/usr/local/mysql/bin/mysqldump -u%s -p%s -h%s -P%s --single-transaction --set-gtid-purged=OFF %s"
        MysqlCommand = MysqlCommand % (self.SourceUser, self.SourcePasswd, self.SourceHost, self.SourcePort, self.ArchiveDb)
        TableList = self.GetTableList()
        for TableName in TableList:
            MysqlCommand += ' ' + TableName[0]

        MysqlCommand += " > /tmp/mysqldump_eltreum_dic_.sql"
        os.popen(MysqlCommand)

    def RestoreData(self):
        MysqlCommand = "/usr/local/mysql/bin/mysql -u%s -p%s -h%s -P%s %s < /tmp/mysqldump_eltreum_dic_.sql"
        MysqlCommand = MysqlCommand % (self.TargetUser, self.TargetPasswd, self.TargetHost, self.TargetPort, self.TargetDb)

        DeleteCommand = "rm /tmp/mysqldump_eltreum_dic_.sql"
        os.popen(MysqlCommand)
        os.popen(DeleteCommand)

if __name__ == '__main__':
    SyncData().GetDump()
    SyncData().RestoreData()
