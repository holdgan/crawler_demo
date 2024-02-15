
import collections
import json
import random
import time

import pandas as pd
import requests
import sqlalchemy as db


from bs4 import BeautifulSoup 



def get_fund_json(to_file=False):
    """抓取"""
    url = "https://news.futunn.com/main/live-list?page=0&page_size=50&_=1654410785355"


# :authority: news.futunn.com
# :method: GET
# :path: /main/live-list?page=0&page_size=50&_=1654410785355
# :scheme: https
# accept: application/json, text/javascript, */*; q=0.01
# accept-encoding: gzip, deflate, br
# accept-language: zh-CN,zh;q=0.9,en;q=0.8
# cookie: news-locale=zh-cn; _csrf=xopkGAxB0r2zNOgZwaji5FaP6DRi1T9V; cipher_device_id=1654410296176494; device_id=1654410296176494; sajssdk_2015_cross_new_user=1; Hm_lvt_f3ecfeb354419b501942b6f9caf8d0db=1654410297; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22ftv1lCVuFxitgpEs401TXYLk1sYQ9qLqJTMxHdNZ%2FONrqYyeXMZCuKBNOoZahAOAsyXt%22%2C%22first_id%22%3A%221813289bc564b7-044952ce106f3fc-1d525635-1296000-1813289bc577d1%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%221813289bc564b7-044952ce106f3fc-1d525635-1296000-1813289bc577d1%22%7D; Hm_lpvt_f3ecfeb354419b501942b6f9caf8d0db=1654410389; tgw_l7_route=aea4e19ff323399c6768100ed4c3452f
# referer: https://news.futunn.com/main/live?lang=zh-cn
# sec-ch-ua: " Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"
# sec-ch-ua-mobile: ?0
# sec-ch-ua-platform: "macOS"
# sec-fetch-dest: empty
# sec-fetch-mode: cors
# sec-fetch-site: same-origin
# user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36
# x-requested-with: XMLHttpRequest


    resp = requests.get(
        url,
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            "cookie": "news-locale=zh-cn; _csrf=xopkGAxB0r2zNOgZwaji5FaP6DRi1T9V; cipher_device_id=1654410296176494; device_id=1654410296176494; sajssdk_2015_cross_new_user=1; Hm_lvt_f3ecfeb354419b501942b6f9caf8d0db=1654410297; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22ftv1lCVuFxitgpEs401TXYLk1sYQ9qLqJTMxHdNZ%2FONrqYyeXMZCuKBNOoZahAOAsyXt%22%2C%22first_id%22%3A%221813289bc564b7-044952ce106f3fc-1d525635-1296000-1813289bc577d1%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%221813289bc564b7-044952ce106f3fc-1d525635-1296000-1813289bc577d1%22%7D; Hm_lpvt_f3ecfeb354419b501942b6f9caf8d0db=1654410310; tgw_l7_route=c63808a9490ee953df1166b7a4165d35",
            "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "macOS",
            "sec-fetch-dest": "document",
            "sec-fetch-site": "navigate",
            "sec-fetch-mode": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }
    )
    jsontext = resp.text  # 重定向到 json 文件可以格式化查看

    # resp = requests.get(url) 
    soup = BeautifulSoup(resp.content, 'html.parser') 

    # all_products = [] 

    products = soup.select('ul.news-list')

    # products = soup.select('li')

    print(products)

    for product in products: 
        rank0 = product.select('span')
        # rank1 = product.select('td')[1].text 
        # rank2 = product.select('td')[2].text 
        # rank3 = product.select('td')[3].text 
        # rank4 = product.select('td')[4].text 
        # rank5 = product.select('td')[5].text 
        # rank6 = product.select('td')[6].text 

        # if rank0 == '代码':
        #     continue
        # if rank0 == '基金代码':
        #     continue        
        # if rank1 == '基金名称':
        #     continue
        # if rank1 == '基金代码':
        #     continue
        # if rank2 == '基金名称':
        #     continue
        # if rank0.isdigit() != True:
        #     continue

        # print(rank0)


        
        # rank7 = product.select('td')[7].text 
        # rank8 = product.select('td')[8].text



        # save_jjzc_mysql(rank0,rank1,rank2,rank3,rank4,rank5,rank6,rank7,rank8)


    if to_file:  # 写到本地 json 文件方便调试
        filename = "./{}.json".format(reportdate)
        with open(filename, "w") as f:
            json.dump(jsontext, f, indent=2, ensure_ascii=False)

    return jsontext


def save_jjzc_mysql(code, code_name, date, jgnum, num, p, vsnum, pre_p, pre_jgnum):
    query = db.insert(Table_jjzc).values(
        code=code,
        code_name=code_name,
        date=date,
        jgnum=jgnum,
        num=num,
        p=p,
        vsnum=vsnum,
        pre_p=pre_p,
        pre_jgnum=pre_jgnum,
    )
    Connection.execute(query)

def init_conn():
    global Connection, Table_jjzc # 全局使用
    url = "mysql+pymysql://root:123456@127.0.0.1:3306/fund"  # 测试地址，改成你的本地 mysql 数据库地址
    # url = "mysql+pymysql://root:wnnwnn@127.0.0.1:3306/testdb"  # 测试地址，改成你的本地 mysql 数据库地址
    engine = db.create_engine(url)
    metadata = db.MetaData()
    Connection = engine.connect()
    Table_jjzc = db.Table('jjzc', metadata, autoload=True, autoload_with=engine)
    # Table_snap = db.Table('fund_snap', metadata, autoload=True, autoload_with=engine)


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


def main():

    # for a in range(100):
        # s=a+1
    get_fund_json() 

    # 抓取所有我关注的雪球上的基金到 mysql
    # crawl_all_my_funds_to_mysql() 

    # 整理基金里的数据
    # run_save_snap()

    """导出excel表格，不适用"""
    # export_all_mysql_funds_stocks_to_excel_vertical()  # 导出基金十大重仓股
    # export_all_mysql_funds_stocks_to_excel()  # 导出横版基金十大重仓
    # export_all_stock_funds()  # 导出每个重仓股票分别被多少基金持有


if __name__ == "__main__":
    # init_conn()
    main()
