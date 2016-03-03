#coding=utf-8
import re
import requests
from lxml import etree
import pymongo
import time
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import sys

#发送邮件方法
def send_mail(info):
    sender = 'siyuan@longcredit.com'  
    receiver =['liuyanwei@longcredit.com']
    subject = '新发地价格采集报告'  
    smtpserver = 'smtp.longcredit.com'  
    username = 'siyuan@longcredit.com'  
    password = ''
#内容配置 
    msg = MIMEText(info.encode('utf8'),_subtype = 'html',_charset ='utf-8') 
    msg['Subject'] = Header(subject, 'utf-8')  

    smtp = smtplib.SMTP()  
    smtp.connect(smtpserver)  
    smtp.login(username, password)  
    smtp.sendmail(sender, receiver, msg.as_string())  
    smtp.quit()  

#连接数据库
def conn_mongo():
    try:
        conn = pymongo.MongoClient("192.168.0.15",27017)
        db = conn.price
        return db
    except:
        send_mail('数据库连接失败,请检查数据库连接！')
        sys.exit(0)

#数据插入方法
def mongo_insert(content):
    try:
        db = conn_mongo()
        tb=db.xfdjg_ts  #测试表，部署需要修改
        tb.insert(content)
    except:
        send_mail('数据库插入失败,请检查！')
        sys.exit(0)
        
#查询数据库中最大时间
def mongo_maxdate():
    try:
        db = conn_mongo()
        tb=db.xfdjg_ts  #测试表，部署需要修改
        row= tb.find({},{'date':1}).sort('date',pymongo.DESCENDING).limit(1)
        for each in row:
            maxdate=each
        return maxdate['date']            
    except:
        return '2016-01-01'
        

#获取URL地址方法
def get_url(page):
    try:
        urllist=[]
        for i in range(1,page+1): 
            url='http://www.xinfadi.com.cn/marketanalysis/1/list/'+str(i)+'.shtml'
            urllist.append(url)
        return urllist
    except:
        print('生成地址失败,请检查传入参数！')
        sys.exit(0)

#分析采集地址中的数据        
def proc_url(url,maxdate):
    try:
        head={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:44.0) Gecko/20100101 Firefox/44.0'}    
        html=requests.get(each,headers=head)
        source=re.search('<table class="hq_table">(.*?)</table>',html.text,re.S).group(1)    
        selector=etree.HTML(source)
    except:
        send_mail('解析网页地址出现问题...T^T..')
        
    li=[]
    try:
        for f in range(2,22):
            content={}
            content['name'] = selector.xpath('//tr['+str(f)+']/td[1]/text()')[0]
            content['min'] = selector.xpath('//tr['+str(f)+']/td[2]/text()')[0]
            content['avg'] = selector.xpath('//tr['+str(f)+']/td[3]/text()')[0]
            content['max'] = selector.xpath('//tr['+str(f)+']/td[4]/text()')[0]
            content['guige'] = selector.xpath('//tr['+str(f)+']/td[5]/text()')[0]
            content['unit'] = selector.xpath('//tr['+str(f)+']/td[6]/text()')[0]
            content['date'] = selector.xpath('//tr['+str(f)+']/td[7]/text()')[0]
            #判断采集的数据是否大于已经入库的数据，大于的才采集
            if content['date']>maxdate:    
                li.append(content)
            else:
                break
    except:
        send_mail ('抓取网页信息失败..')
    return li
              
        
if __name__=='__main__':
    #获取数据库中已采集数据的时间
    maxdate=mongo_maxdate()
    #获取前100个页面的URL
    url=get_url(100)
    print('获取网页列表成功..')
    #计数器
    i=0
    #循环100个URL地址
    for each in url:
        print('正在处理页面..'+each)
        #采集地址里面的内容，将内容返回一个列表
        d=proc_url(each,maxdate)
        #判断返回列表长度，如果非空：执行插入
        #如果空列表：说明已经没可采集内容，提示采集完成并发送邮件，退出程序
        print(len(d))
        if len(d)>0:
            for row in d:
                i+=1
                mongo_insert(row)
        else:
            info='采集完成,采集数量'+str(i)+'条'
            print(info)
            #to_user=['wangmeng6@qq.com','liuyanwei@longcredit.com']
            send_mail(info)
            sys.exit(0)