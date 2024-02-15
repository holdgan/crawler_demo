# -*- coding: utf-8 -*-

"""
抓取蛋卷基金数据。主要是为了导出我关注的一些优秀主动基金的季度重仓股。随手写的仅供参考
pip install requests
pip install SQLAlchemy -i https://pypi.doubanio.com/simple --user
pip install pymysql -i https://pypi.doubanio.com/simple --user
# https://github.com/numpy/numpy/issues/15947 numpy 版本高 mac 有问题
pip3 install numpy==1.18.0 -i https://pypi.doubanio.com/simple
pip3 install pandas -i https://pypi.doubanio.com/simple
pip3 install openpyxl -i https://pypi.doubanio.com/simple
"""

import collections
import json
import random
import time

import pandas as pd
import requests
import sqlalchemy as db

"""
curl 'https://danjuanfunds.com/djapi/fund/detail/110011' \
  -H 'Connection: keep-alive' \
  -H 'Accept: application/json, text/plain, */*' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36' \
  -H 'elastic-apm-traceparent: 00-ce58f82d3f8e8e6fcd54397fd0f38574-adb2e157029a1192-01' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Referer: https://danjuanfunds.com/funding/110011?channel=1300100141' \
  -H 'Accept-Language: zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7' \
  -H 'Cookie: device_id=web_SkS2df508; _ga=GA1.2.1397406205.1593612461; gr_user_id=2ac24d8b-927e-475d-8f29-27589058f70f; Hm_lvt_d8a99640d3ba3fdec41370651ce9b2ac=1602344637,1602344758,1602344764,1603369369; acw_tc=2760822016038852981828649e5a0c60b368285cb530c1a2e8d169c1867d83; xq_a_token=c2974070ad952835feab798d5278f70696c9f25c; Hm_lpvt_d8a99640d3ba3fdec41370651ce9b2ac=1603885299; channel=1300100141; timestamp=1603885317702' \
  --compressed
"""


def get_fund_json(fund_code, fund_name="", to_file=False):
    """抓取蛋卷基金数据"""
    url = "https://danjuanfunds.com/djapi/fund/detail/{}".format(fund_code)
    print("抓取:{} {}".format(fund_code, fund_name))
    resp = requests.get(
        url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Referer": "https://danjuanfunds.com/funding/110011?channel=1300100141",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
            "elastic-apm-traceparent": "00-ce58f82d3f8e8e6fcd54397fd0f38574-adb2e157029a1192-01"},
        cookies={
            "Hm_lpvt_d8a99640d3ba3fdec41370651ce9b2ac": "1603885299",
            "Hm_lvt_d8a99640d3ba3fdec41370651ce9b2ac": "1602344637,1602344758,1602344764,1603369369",
            "_ga": "GA1.2.1397406205.1593612461",
            "acw_tc": "2760822016038852981828649e5a0c60b368285cb530c1a2e8d169c1867d83",
            "channel": "1300100141",
            "device_id": "web_SkS2df508",
            "gr_user_id": "2ac24d8b-927e-475d-8f29-27589058f70f",
            "timestamp": "1603885317702",
            "xq_a_token": "c2974070ad952835feab798d5278f70696c9f25c"},
    )
    jsontext = resp.text  # 重定向到 json 文件可以格式化查看

    if to_file:  # 写到本地 json 文件方便调试
        filename = "./{}.json".format(fund_code)
        with open(filename, "w") as f:
            json.dump(resp.json(), f, indent=2, ensure_ascii=False)

    return jsontext

def get_fund_djapi_json(fund_code, fund_name="", to_file=False):
    """抓取蛋卷基金数据""" 
    url = "https://danjuanapp.com/djapi/fund/{}".format(fund_code)
    print("抓取:{} {}".format(fund_code, fund_name))
    resp = requests.get(
        url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Referer": "https://danjuanfunds.com/funding/110011?channel=1300100141",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
            "elastic-apm-traceparent": "00-ce58f82d3f8e8e6fcd54397fd0f38574-adb2e157029a1192-01"},
        cookies={
            "Hm_lpvt_d8a99640d3ba3fdec41370651ce9b2ac": "1603885299",
            "Hm_lvt_d8a99640d3ba3fdec41370651ce9b2ac": "1602344637,1602344758,1602344764,1603369369",
            "_ga": "GA1.2.1397406205.1593612461",
            "acw_tc": "2760822016038852981828649e5a0c60b368285cb530c1a2e8d169c1867d83",
            "channel": "1300100141",
            "device_id": "web_SkS2df508",
            "gr_user_id": "2ac24d8b-927e-475d-8f29-27589058f70f",
            "timestamp": "1603885317702",
            "xq_a_token": "c2974070ad952835feab798d5278f70696c9f25c"},
    )
    jsontext_djapi = resp.text  # 重定向到 json 文件可以格式化查看

    if to_file:  # 写到本地 json 文件方便调试
        filename = "./{}.json".format(fund_code)
        with open(filename, "w") as f:
            json.dump(resp.json(), f, indent=2, ensure_ascii=False)

    return jsontext_djapi


Connection = None
Table_datas = None
Table_snap = None


def init_conn():
    global Connection, Table_datas, Table_snap # 全局使用
    url = "mysql+pymysql://root:123456@127.0.0.1:3306/fund"  # 测试地址，改成你的本地 mysql 数据库地址
    # url = "mysql+pymysql://root:wnnwnn@127.0.0.1:3306/testdb"  # 测试地址，改成你的本地 mysql 数据库地址
    engine = db.create_engine(url)
    metadata = db.MetaData()
    Connection = engine.connect()
    Table_datas = db.Table('fund_datas', metadata, autoload=True, autoload_with=engine)
    Table_snap = db.Table('fund_snap', metadata, autoload=True, autoload_with=engine)


def fund_name_from_manager_list(fund_code, achievement_list):
    """每个管理者都会管理很多基金，找到当前这个基金并返回名字"""
    for fund in achievement_list:
        if fund.get('fund_code') == fund_code:
            return fund['fundsname']
    return ""


def get_managers(manager_list):
    """找到所有基金管理人返回空格分割字符串 eg: 李晓星 张坤"""
    name_list = []
    for manager in manager_list:
        name_list.append(manager['name'])
    return " ".join(name_list)



# 在你的 mysql 创建这个表
"""
CREATE TABLE `fund_datas` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `fund_name` varchar(64) DEFAULT '' COMMENT '基金名称',
  `fund_code` varchar(16) NOT NULL DEFAULT '' COMMENT '基金代码',
  `managers` varchar(32) NOT NULL DEFAULT '' COMMENT '管理人',
  `enddate` varchar(32) NOT NULL DEFAULT '' COMMENT '季报日期',
  `found_date` varchar(32) NOT NULL DEFAULT '' COMMENT '创始时间',
  `totshare` bigint(50) NOT NULL COMMENT '基金规模',
  `detail_json` text NOT NULL COMMENT '蛋卷基金详细信息 json',
  `djapi_json` text NOT NULL COMMENT '蛋卷基金详细信息 json',
  PRIMARY KEY (`id`),
  UNIQUE KEY `code_enddate` (`fund_code`,`enddate`),
  KEY `idx_code` (`fund_code`),
  KEY `idx_name` (`fund_name`)
) ENGINE=InnoDB AUTO_INCREMENT=36 DEFAULT CHARSET=utf8mb4;
"""

def parse_fund_datas(fund_code, json_text, json_text_djapi):
    d = json.loads(json_text)
    data = d['data']

    manager_list = data['manager_list']
    achievement_list = manager_list[0]['achievement_list']  # 找到一个管理者

    fund_name = fund_name_from_manager_list(fund_code, achievement_list)
    managers = get_managers(manager_list)
    try:
        enddate = data['fund_position']['enddate']  # 季报披露日期
    except KeyError:
        enddate = ""


    d_djapi = json.loads(json_text_djapi)
    data_djapi = d_djapi['data']

    try:
        found_date = data_djapi['found_date']
    except KeyError:
        found_date = ""
    totshare = data_djapi['totshare'].replace('亿','00000000')
    # yields = data_djapi['fund_derived']['yield_history']
    # try:
    #     yield_3 = yields[0]['yield']
    # except KeyError:
    #     yield_3 = ""
    # try:
    #     yield_6 = yields[1]['yield']
    # except KeyError:
    #     yield_6 = ""
    # try:
    #     yield_12 = yields[2]['yield']
    # except KeyError:
    #     yield_12 = ""
    # try:
    #     yield_24 = yields[3]['yield']
    # except KeyError:
    #     yield_24 = ""
    # try:
    #     yield_36 = yields[4]['yield']
    # except KeyError:
    #     yield_36 = ""
    # try:
    #     yield_60 = yields[5]['yield']
    # except KeyError:
    #     yield_60 = ""
    # try:
    #     yield_create = yields[6]['yield']
    # except KeyError:
    #     yield_create = ""
 

    return fund_name, managers, enddate, found_date, totshare



def save_datas_mysql(fund_code, fund_name, managers, enddate, found_date, totshare, detail_json, djapi_json):
    query = db.insert(Table_datas).values(
        fund_name=fund_name,
        fund_code=fund_code,
        managers=managers,
        enddate=enddate,
        found_date=found_date,
        totshare=totshare,
        detail_json=detail_json,
        djapi_json=djapi_json,
    )
    Connection.execute(query)

def request_and_save_datas(fund_code, fund_name):
    json_text = get_fund_json(fund_code, fund_name)
    json_text_djapi = get_fund_djapi_json(fund_code, fund_name)
    fund_name, managers, enddate, found_date, totshare = parse_fund_datas(fund_code, json_text, json_text_djapi)
    save_datas_mysql(fund_code, fund_name, managers, enddate, found_date, totshare, json_text, json_text_djapi)

"""
CREATE TABLE `fund_snap` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `enddate` varchar(32) NOT NULL DEFAULT '' COMMENT '季报日期',
  `fund_name` varchar(64) DEFAULT '' COMMENT '基金名称',
  `fund_code` varchar(16) NOT NULL DEFAULT '' COMMENT '基金代码',
  `fund_scale` bigint(50) NOT NULL COMMENT '基金规模',
  `stock_name` varchar(32) NOT NULL DEFAULT '' COMMENT '重仓股名称',
  `stock_code` varchar(32) NOT NULL DEFAULT '' COMMENT '重仓股代码',
  `stock_per` decimal(32,8) NOT NULL COMMENT '重仓股占比',
  `stock_scale` decimal(32,8) NOT NULL COMMENT '重仓股大致规模',
  PRIMARY KEY (`id`),
  UNIQUE KEY `fund_code` (`fund_code`,`stock_code`,`enddate`)
) ENGINE=InnoDB AUTO_INCREMENT=332 DEFAULT CHARSET=utf8mb4;
"""

def get_mysql_fund_json():
    query = db.select([Table_datas]).order_by(db.desc(Table_datas.columns.id)).limit(100)
    rows = Connection.execute(query).fetchall()
    fund_dict = []
    i = 0
    for row in rows:
        d = json.loads(row.detail_json)
        try:
            stock_list = d['data']['fund_position']['stock_list']
        except KeyError:  # 新基金没披露可能为空
            stock_list = []

        for stock in stock_list:
            # name, code, percent = stock['name'], stock['code'], stock['percent']
            stocks = {}
            # vals = [row.enddate, row.fund_name, row.fund_code, 0, name, code, percent, 0]
            stocks['enddate']=row.enddate
            stocks['fund_name']=row.fund_name
            stocks['fund_code']=row.fund_code
            stocks['fund_scale']=row.totshare
            stocks['stock_name']=stock['name']
            stocks['stock_code']=stock['code']
            stocks['stock_per']=stock['percent']/100
            stocks['stock_scale']=row.totshare*stock['percent']/100

            # stocks.append((enddate, fund_name, fund_code, fund_scale, stock_name, stock_code, stock_per, stock_scale))
            fund_dict.append(stocks)

            i = i + 1

            # key = row.fund_name
            # vals = [row.fund_code, row.managers, row.enddate] + stocks
            # fund_dict[key] = vals

    return fund_dict

def save_snap_mysql(json_text):
    # print(json_text)

    for row in json_text:

        print(row)

        query = db.insert(Table_snap).values(
            enddate=row['enddate'],
            fund_name=row['fund_name'],
            fund_code=row['fund_code'],
            fund_scale=row['fund_scale'],
            stock_name=row['stock_name'],
            stock_code=row['stock_code'],
            stock_per=row['stock_per'],
            stock_scale=row['stock_scale'],
        )
        Connection.execute(query)


def run_save_snap():
    json_text = get_mysql_fund_json()
    save_snap_mysql(json_text)




def get_my_xueqiu_fund_codes():
    """ 从我的雪球获取我关注的所有基金代码。如果你有雪球账号并且有关注的基金，可以用这段代码自动化查询。
    https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?size=1000&pid=-110&category=2
    """
    # resp = requests.get(
    #     "https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?size=1000&pid=-110&category=2",
    #     headers={
    #         "Accept": "application/json, text/plain, */*",
    #         "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    #         "Connection": "keep-alive",
    #         "Origin": "https://xueqiu.com",
    #         "Referer": "https://xueqiu.com/",
    #         "Sec-Fetch-Dest": "empty",
    #         "Sec-Fetch-Mode": "cors",
    #         "Sec-Fetch-Site": "same-site",
    #         "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"},
    #     cookies={}, # TODO 用浏览器查询请求，改成你自己的 uncurl requests 代码，注意你的 cookie 不要随便泄露出去
    # )
    #
    # with open("./funds.json", "w") as f:
    #     json.dump(resp.json(), f, indent=2, ensure_ascii=False)
    #
    codes = []  # [ (code, name) ]
    with open("./funds.json") as f:
        res = json.load(f)
        stocks = res['data']['stocks']
        for stock in stocks:
            symbol = stock['symbol']
            fund_code = ''.join(char for char in symbol if char.isdigit())
            fund_name = stock['name']
            codes.append((fund_code, fund_name))
    return codes


def crawl_all_my_funds_to_mysql():
    funds = get_my_xueqiu_fund_codes()
    __import__('pprint').pprint(funds)

    for fund_code, fund_name in funds:
        request_and_save_datas(fund_code, fund_name)
        time.sleep(random.randint(5, 10))  # 注意慢一点，随机 sleep 防止命中反作弊


def export_all_mysql_funds_stocks_to_dict():
    """导出所有基金的股票前十大重仓股票到 excel"""
    query = db.select([Table_datas]).order_by(db.desc(Table_datas.columns.id)).limit(100)
    rows = Connection.execute(query).fetchall()
    fund_dict = {}
    for row in rows:
        d = json.loads(row.detail_json)
        try:
            stock_list = d['data']['fund_position']['stock_list']
        except KeyError:  # 新基金没披露可能为空
            stock_list = []

        stocks = []
        for stock in stock_list:
            name, code, percent = stock['name'], stock['code'], stock['percent']
            stock_fmt = u"{}[{}]({}%)".format(name, code, percent)
            stocks.append(stock_fmt)

        if len(stocks) < 10: # 十大重仓股
            stocks += (10 - len(stocks)) * [""]

        key = row.fund_name
        vals = [row.fund_code, row.managers, row.enddate] + stocks
        fund_dict[key] = vals

    return fund_dict


# https://www.geeksforgeeks.org/how-to-create-dataframe-from-dictionary-in-python-pandas/
def export_all_mysql_funds_stocks_to_excel_vertical():
    fund_dict = export_all_mysql_funds_stocks_to_dict()
    index = ['代码', '管理人', '季报日期'] + ['重仓股'] * 10  # 十大重仓
    df = pd.DataFrame(fund_dict, index=index)
    df.to_excel("./funds_stock_vertical.xlsx")


def export_all_mysql_funds_stocks_to_excel():  # 横着
    fund_dict = export_all_mysql_funds_stocks_to_dict()
    # index = ['代码', '管理人', '季报日期'] + ['重仓股'] * 10
    df = pd.DataFrame.from_dict(fund_dict, orient='index')
    df.to_excel("./fund_stock.xlsx")


def export_all_stock_funds():
    """导出每个股票被多少基金持有，比较容易看出哪些股票被抱团"""
    query = db.select([Table_datas]).order_by(db.desc(Table_datas.columns.id)).limit(100)
    rows = Connection.execute(query).fetchall()
    stock_funds = collections.defaultdict(list)
    for row in rows:
        d = json.loads(row.detail_json)
        try:
            stock_list = d['data']['fund_position']['stock_list']
        except KeyError:  # 新基金没披露可能为空
            stock_list = []

        for stock in stock_list:
            name, code, percent = stock['name'], stock['code'], stock['percent']
            stock_name = u"{}({})".format(name, code)
            stock_funds[stock_name].append(row.fund_name)

    keys = sorted(stock_funds, key=lambda k: len(stock_funds[k]), reverse=True)
    sorted_stock_dict = {k: stock_funds[k] for k in keys}
    df = pd.DataFrame.from_dict(sorted_stock_dict, orient='index')
    df.to_excel("./stock.xlsx")


def main():
    # get_fund_json("007300", "汇添富中盘", True) # 单独抓取一个基金数据到文件
    # 抓取所有我关注的雪球上的基金到 mysql
    # crawl_all_my_funds_to_mysql() 

    # 整理基金里的数据
    run_save_snap()

    """导出excel表格，不适用"""
    # export_all_mysql_funds_stocks_to_excel_vertical()  # 导出基金十大重仓股
    # export_all_mysql_funds_stocks_to_excel()  # 导出横版基金十大重仓
    # export_all_stock_funds()  # 导出每个重仓股票分别被多少基金持有


if __name__ == "__main__":
    init_conn()
    main()
