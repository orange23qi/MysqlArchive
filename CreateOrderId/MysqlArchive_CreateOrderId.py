# -*-coding: utf-8-*-

import ConfigParser
import pymysql
import datetime
import time
from dateutil.relativedelta import relativedelta


class MysqlArchive(object):
    def __init__(self):
        Now = datetime.datetime.now()
        self.TodayMin = datetime.datetime.combine(Now, datetime.time.min)
        self.UnixTodayMin = time.mktime(self.TodayMin.timetuple())
        self.Today = datetime.datetime.now().date().strftime("%Y-%m-%d")

        config = ConfigParser.RawConfigParser()
        config.read('/data/mysql/scripts/MysqlArchiveScripts/Config/Userorder.cfg')
        self.OrigHost = config.get('Userorder', 'ORIG_HOST')
        self.OrigUser = config.get('Userorder', 'ORIG_USER')
        self.OrigPasswd = config.get('Userorder', 'ORIG_PASSWD')
        self.OrigDb = config.get('Userorder', 'ORIG_DB')
        self.OrigPort = config.getint('Userorder', 'ORIG_PORT')

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

    def CreateOrigData(self, ArchiveSaveTime):
        SqlCreateOrderData = "INSERT IGNORE INTO archive_order_id_mapping \
                             (order_id, complete_date, country, archive_date) \
                             SELECT id, complete_date, '%s', '%s' \
                             FROM userorder_%s.user_order a \
                             WHERE complete_date > 0 AND complete_date < %d \
                             AND NOT EXISTS(SELECT 1 FROM \
                             archive_order_id_mapping b \
                             WHERE a.id = b.order_id);"
        StartDay = self.UnixTodayMin - ArchiveSaveTime * 24 * 60 * 60

        for Country in ["id", "th", "my", "sg", "pk", "twc"]:
            self.ExecSql(SqlCreateOrderData % (Country, self.Today, Country,
                         StartDay), self.OrigHost, self.OrigUser,
                         self.OrigPasswd, self.OrigDb, self.OrigPort)

    def UpdateCompleteTable(self):
        SqlUpdateCompleteTable = "INSERT IGNORE INTO order_id_complete_log \
                                 (complete_date) VALUE ('%s')"
        SqlUpdateCompleteTable = SqlUpdateCompleteTable % self.Today

        self.ExecSql(SqlUpdateCompleteTable, self.OrigHost, self.OrigUser,
                     self.OrigPasswd, self.OrigDb, self.OrigPort)


if __name__ == '__main__':
    """
    根据时间,从userorder_xx库获取需要归档的order_id
    """
    StartDay = datetime.date.today() - relativedelta(months=+3)
    Today = datetime.date.today()
    Interval = Today - StartDay

    MysqlArchive().CreateOrigData(Interval.days)
    MysqlArchive().UpdateCompleteTable()
