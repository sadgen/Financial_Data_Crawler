# -*- coding: utf-8 -*-

import pymysql
from twisted.enterprise import adbapi
from scrapy.utils.project import get_project_settings  #导入seetings配置

class DBHelper():
    '''这个类也是读取settings中的配置，自行修改代码进行操作'''

    def __init__(self):
        settings = get_project_settings()  #获取settings配置，设置需要的信息

        dbparams = dict(
            host=settings['MYSQL_HOST'],  #读取settings中的配置
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',  #编码要加上，否则可能出现中文乱码问题
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True,
        )
        #**表示将字典扩展为关键字参数,相当于host=xxx,db=yyy....
        dbpool = adbapi.ConnectionPool('pymysql', **dbparams)

        self.dbpool = dbpool

    def connect(self):
        return self.dbpool

    #创建数据库
    def insert(self, item):
        #调用插入的方法
        query = self.dbpool.runInteraction(self._conditional_insert, item)
        #调用异常处理方法
        query.addErrback(self._handle_error)
        return item

    def mapping_df_types(self, df):
        dtypedict = {}
        for i, j in zip(df.columns, df.dtypes):
            if "object" in str(j):
                dtypedict.update({i: " VARCHAR(255) "})
            if "float" in str(j):
                dtypedict.update({i: " FLOAT "})
            if "int" in str(j):
                dtypedict.update({i: " INTEGER "})
            if "datetime" in str(j):
                dtypedict.update({i: " DATETIME "})
        return dtypedict

    #写入数据库中
    def _conditional_insert(self, tx, item):
        itemtag = item['tag']
        # 先构造需要的或是和数据库相匹配的列
        columns = list(item[itemtag].columns)
        # 可以删除不要的列或者数据库没有的列名
        # 重新构造df,用上面的columns,到这里你要保证你所有列都要准备往数据库写入了
        new_df = item[itemtag][columns].copy()
        # 构造符合sql语句的列，因为sql语句是带有逗号分隔的,(这个对应上面的sql语句的(column1, column2, column3))
        columns = ','.join(list(new_df.columns))
        # 构造每个列对应的数据，对应于上面的((value1, value2, value3))
        data_list = [tuple(i) for i in new_df.values] # 每个元组都是一条数据，根据df行数生成多少元组数据
        # 计算一行有多少value值需要用字符串占位
        s_count = len(data_list[0]) * "%s,"
        dtypes_list = self.mapping_df_types(item[itemtag])
        # 构造sql语句
        r = ','.join(["`" + str(x) + "` " + dtypes_list[x] + " " for x in columns.split(',')])
        sql = "create table if not exists `" + itemtag + "` (" + r + ")ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        tx.execute(sql)
        insert_sql = "replace into " + itemtag + " (" + columns + ") values (" + s_count[:-1] + ")"
        tx.executemany(insert_sql, data_list)


    def _handle_error(self, failue):
        print('--------------database operation exception!!-----------------')
        print(failue)
