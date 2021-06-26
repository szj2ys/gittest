# *_*coding:utf-8 *_*
"""
Descri：推送邮件给客户
"""
import datetime
import time

from settings.envs import *
from utils import time_convert, color_table
from utils.io import table_blue, two_diff_color_table
from pigeomail import Message, Mailer
import mysqling
from chinese_calendar import is_workday
from settings.mailconfig import RECEIVERS
from settings.paths import dirs
import sys
sys.path.append(str(dirs.ROOT))


def get_topn(df, topn):
    for i, row in df.iterrows():
        if int(row["senti_rank"]) <= topn:
            yield [row["secuname"], row["senti"], row["senti_rank"], row["title"]]


def get_topn_news_count(df, topn):
    for i, row in df.iterrows():
        if int(row["senti_rank"]) <= topn:
            yield [row["secuname"], row["news_count"]]

if __name__ == "__main__":
    start = time.time()
    mysql = mysqling.register(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWD,
        db=MYSQL_DB,
    )
    SQL_POS = """
SELECT DISTINCT * FROM 
( 
SELECT 
        secuname,
        secucode,
--      articlecode,
        COUNT( *) AS news_count,   -- 股票相关资讯条数
        AVG( senti) AS avg_senti,
        publ_time,
        ROW_NUMBER() OVER (ORDER BY SUM(senti) DESC) AS senti_rank  -- 相同时不并列且不跳过
FROM news_senti
WHERE 1=1
    AND STR_TO_DATE(publ_time,'%Y-%m-%d %H:%i:%s') >= DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY) ,'%Y-%m-%d %H:%i:%s') -- 过去24小时的数据  -- 过去24小时的数据
	AND secumarket in (1,3)

        GROUP BY secuname
ORDER BY senti_rank ASC
    ) m 
LEFT JOIN (
        SELECT * FROM (
                SELECT
                        secucode,articlecode,senti
                FROM news_senti
                WHERE 1=1
                    AND STR_TO_DATE(publ_time,'%Y-%m-%d %H:%i:%s') >= DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY) ,'%Y-%m-%d %H:%i:%s') -- 过去24小时的数据
                    AND secumarket in (1,3)
    ) p
    
    LEFT JOIN 
        (SELECT 
            DISTINCT id,title 
         FROM news_senti_news
         WHERE 1=1
            AND DATE_FORMAT(news_publ_time,'%Y-%m-%d') >= DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY) ,'%Y-%m-%d')
        ) k
        ON p.articlecode=k.id
) n
ON m.secucode= n.secucode
ORDER BY senti_rank ASC 
    """

    SQL_NEG = """
SELECT DISTINCT * FROM 
( 
SELECT 
        secuname,
        secucode,
--      articlecode,
        COUNT( *) AS news_count,   -- 股票相关资讯条数
        AVG( senti) AS avg_senti,
        publ_time,
        ROW_NUMBER() OVER (ORDER BY SUM(senti) ASC) AS senti_rank  -- 相同时不并列且不跳过
FROM news_senti
WHERE 1=1
    AND STR_TO_DATE(publ_time,'%Y-%m-%d %H:%i:%s') >= DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY) ,'%Y-%m-%d %H:%i:%s') -- 过去24小时的数据  -- 过去24小时的数据
	AND secumarket in (1,3)

        GROUP BY secuname
ORDER BY senti_rank ASC
    ) m 
LEFT JOIN (
        SELECT * FROM (
                SELECT
                        secucode,articlecode,senti
                FROM news_senti
                WHERE 1=1
                    AND STR_TO_DATE(publ_time,'%Y-%m-%d %H:%i:%s') >= DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY) ,'%Y-%m-%d %H:%i:%s') -- 过去24小时的数据
                    AND secumarket in (1,3)
    ) p
    
    LEFT JOIN 
        (SELECT 
            DISTINCT id,title 
         FROM news_senti_news
         WHERE 1=1
            AND DATE_FORMAT(news_publ_time,'%Y-%m-%d') >= DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY) ,'%Y-%m-%d')
        ) k
        ON p.articlecode=k.id
) n
ON m.secucode= n.secucode
ORDER BY senti_rank ASC 
    """

    SQL_POS_NEWSCOUNT = '''
    SELECT 
    DISTINCT 
        secuname,
        secucode,
--      articlecode,
        COUNT( *) AS news_count,   -- 股票相关资讯条数
        AVG( senti) AS avg_senti,
        publ_time,
        ROW_NUMBER() OVER (ORDER BY SUM(senti) DESC) AS senti_rank  -- 相同时不并列且不跳过
FROM news_senti
WHERE 1=1
    AND STR_TO_DATE(publ_time,'%Y-%m-%d %H:%i:%s') >= DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY) ,'%Y-%m-%d %H:%i:%s') -- 过去24小时的数据
	AND secumarket in (1,3)

        GROUP BY secuname
ORDER BY senti_rank ASC
    '''
    SQL_NEG_NEWSCOUNT = '''
    SELECT 
        secuname,
        secucode,
--      articlecode,
        COUNT( *) AS news_count,   -- 股票相关资讯条数
        AVG( senti) AS avg_senti,
        publ_time,
        ROW_NUMBER() OVER (ORDER BY SUM(senti) ASC) AS senti_rank  -- 相同时不并列且不跳过
FROM news_senti
WHERE 1=1
    AND STR_TO_DATE(publ_time,'%Y-%m-%d %H:%i:%s') >= DATE_FORMAT(DATE_SUB(now(), INTERVAL 1 DAY) ,'%Y-%m-%d %H:%i:%s') -- 过去24小时的数据
	AND secumarket in (1,3)

        GROUP BY secuname
ORDER BY senti_rank ASC
    '''
    df_pos, _ = mysql.select_as_df(command=SQL_POS)
    df_neg, _ = mysql.select_as_df(command=SQL_NEG)

    df_pos_news_count, _ = mysql.select_as_df(command=SQL_POS_NEWSCOUNT)
    df_neg_news_count, _ = mysql.select_as_df(command=SQL_NEG_NEWSCOUNT)


    # # -------------------------------------------------
    msg = Message()
    msg.From = "佳兆业金融科技"
    msg.To = RECEIVERS
    msg.Cc = []


    # topn senti data
    cols = ["股票", "情绪", "排名", "标题"]  # columns
    data_pos = [row for row in get_topn(df_pos, 10)]
    data_neg = [row for row in get_topn(df_neg, 10)]

    # topn senti news_count
    news_count_cols = ["股票", "声量"]  # columns
    news_count_data_pos = [row for row in get_topn_news_count(df_pos_news_count, 10)]
    news_count_data_neg = [row for row in get_topn_news_count(df_neg_news_count, 10)]

    if is_workday(datetime.datetime.now()):
        mailer = Mailer(
            host=MAIL_SERVER_HOST,
            port=MAIL_SERVER_PORT,
            user=MAIL_SERVER_USER,
            pwd=MAIL_SERVER_PWD,
        )
        msg.Subject = "每日舆情监控——正面舆情统计表"
        msg.Html = color_table(cols=cols, data=data_pos, style='green')
        # msg.Html = table_blue(cols=cols, data=data_pos, name="正面舆情统计表")
        mailer.send(msg)

        msg.Subject = "每日舆情监控——负面舆情统计表"
        msg.Html = color_table(cols=cols, data=data_neg, style='red')
        # msg.Html = table_blue(cols=cols, data=data_neg, name="负面舆情统计表")
        mailer.send(msg)


        msg.Subject = "每日舆情监控——舆情声量统计表"
        msg.Html = two_diff_color_table(cols1=news_count_cols, data1=news_count_data_pos,cols2=news_count_cols, data2=news_count_data_neg, style1='green', style2='red')

        mailer.send(msg)

    print("耗时：{}".format(time_convert(time.time() - start)))
