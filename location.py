from bs4 import BeautifulSoup 
from urllib import request
import re
import pandas as pd
import numpy as np
import urllib.parse as urp
import pandas

class GetAddressInfo(object):
    def __init__(self):    
        self.fieldnames=['小区经度','小区纬度','小区地址']
        self.df = pd.DataFrame(columns=self.fieldnames)   
        
    def lat(self,res):
        try:
            return pd.to_numeric(re.findall('"lat":(.*)',res)[0].split(',')[0])
        except:
            return 0
    def lng(self,res):
        try:
            return pd.to_numeric(re.findall('"lng":(.*)',res)[0])
        except:
            return 0
    def address(self,res):
        try:
            return re.findall('"address":"(.*)",',res)[0]
        except:
            return 'None'
    
    def get_neigbour_address(self,cumunity_name,city='上海'):
        my_ak ='t9bipb2vGERrEzRLAsgoAjGAxybcWQaS' ##替换自己的ak
        qurey = urp.quote(cumunity_name)#小区名称
        tag = urp.quote('住宅区')
        
        url = 'http://api.map.baidu.com/place/v2/search?query='+qurey+'&tag='+tag+'&region='\
        +urp.quote(city)+'&output=json&ak='+my_ak
        req = request.urlopen(url)
        res = req.read().decode()
        lat = self.lat(res)
        lng = self.lng(res)
        #address = self.address(res)
        return lat,lng

    def run(self,name):
        self.get_neigbour_address(cumunity_name=name,city='上海')
        return list(self.get_neigbour_address(cumunity_name=name,city='上海'))
 
# if __name__ == '__main__':
#    GetAddressInfo=GetAddressInfo()
#    GetAddressInfo.run('东方商厦嘉定')