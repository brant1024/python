#coding=utf-8
import re
import requests
from lxml import etree
import pymongo
import time
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from lib2to3.fixer_util import String

#邮件发送函数
def send_mail(info):
    sender = 'liuyanwei@longcredit.com'  
    receiver = 'liuyanwei@longcredit.com,360777790@qq.com'  
    subject = '采集监控报告'  
    smtpserver = 'smtp.qiye.163.com'  
    username = 'liuyanwei@longcredit.com'  
    password = 'Heaven8366!'
    touser = receiver.split(',')
#邮件内容      
    msg = MIMEText(info,'text','utf-8') 
    msg['Subject'] = Header(subject, 'utf-8')  
#邮件配置      
    smtp = smtplib.SMTP()  
    smtp.connect(smtpserver)  
    smtp.login(username, password)  
    smtp.sendmail(sender, touser, msg.as_string())  
    smtp.quit()  

#连接数据库服务器
conn = pymongo.MongoClient("192.168.0.15",27017)
#连接数据库
db = conn.price
#选择数据表
tb=db.bjjgtest

#要采集的页面URL
url='http://m.beijingprice.cn/price01_1.aspx?type=56'
head={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:44.0) Gecko/20100101 Firefox/44.0'}
html=requests.get(url,headers=head)
print('获取网页源代码。。。。')
xxpath=etree.HTML(html.text)
print('分析网页数据。。。。')
num=0
try:
    for f in range(1,99):        
        content={}
        #品种
        content['name'] = xxpath.xpath('.//*[@id="form1"]/section/div[2]/table/tbody/tr['+str(f)+']/td[1]/text()')[0]
        #超市零售价格
        content['max'] = xxpath.xpath('.//*[@id="form1"]/section/div[2]/table/tbody/tr['+str(f)+']/td[2]/text()')[0]
        content['min'] = xxpath.xpath('.//*[@id="form1"]/section/div[2]/table/tbody/tr['+str(f)+']/td[3]/text()')[0]
        content['avg'] = xxpath.xpath('.//*[@id="form1"]/section/div[2]/table/tbody/tr['+str(f)+']/td[4]/text()')[0]
        #农贸零售价格
        content['max1'] = xxpath.xpath('.//*[@id="form1"]/section/div[3]/table/tbody/tr['+str(f)+']/td[2]/text()')[0]
        content['min1'] = xxpath.xpath('.//*[@id="form1"]/section/div[3]/table/tbody/tr['+str(f)+']/td[3]/text()')[0]
        content['avg1'] = xxpath.xpath('.//*[@id="form1"]/section/div[3]/table/tbody/tr['+str(f)+']/td[4]/text()')[0]
        #批发价格
        content['max2'] = xxpath.xpath('.//*[@id="form1"]/section/div[4]/table/tbody/tr['+str(f)+']/td[2]/text()')[0]
        content['min2'] = xxpath.xpath('.//*[@id="form1"]/section/div[4]/table/tbody/tr['+str(f)+']/td[3]/text()')[0]
        content['avg2'] = xxpath.xpath('.//*[@id="form1"]/section/div[4]/table/tbody/tr['+str(f)+']/td[4]/text()')[0]
        #采集日期
        content['cdate']=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        #数据日期
        content['date'] = xxpath.xpath('.//*[@id="form1"]/section/div[2]/table/caption/span/text()')[0]
        tb.insert(content)
        num=num+1
        print(content['name']+'数据成功采集。。。。。')
        #print(content)
except Exception as e:
    err=str(e) 
    pass
print('采集完成')
send_mail('采集完成,有错误,采集记录数量 '+str(num)+' 完成时间'+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))            


    






