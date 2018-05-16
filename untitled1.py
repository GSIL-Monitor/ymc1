#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 16 20:05:08 2018

@author: yinmingchu
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor
This is a temporary script file.
"""

import pandas as pd
import json
from selenium import webdriver 
from selenium.webdriver.common.keys import Keys 
import time 
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
import os




#####Part1 Transform the et doc into the standard json format


def Json_Format(input_path,output_path=""):
    
    def to_int(x):
        try:
            ret = int(x)
        except Exception:
            ret = x
        return(ret)

    def transform_single_param(param):
        ele_split = param.split(':')
        param_name = ele_split[0]
        param_value = ele_split[1].split(',')
        param_value = list(map(to_int,param_value))
        if len(param_value)==1:
            if type(param_value[0])==int:
                ret = ' '*8+'"'+param_name+'":'+str(param_value[0])
            else:
                ret = ' '*8+'"'+param_name+'":"'+str(param_value[0])+'"'
        else:
            if type(param_value[0])==int:
                param_value = list(map(str,param_value))
                ret = (',\n'+' '*12).join(param_value)
                ret = ' '*8+'"'+param_name+'":[\n'+' '*12+ret+'\n'+' '*8+']'
            else:
                ret = ('",\n'+' '*12+'"').join(param_value)
                ret = ' '*8+'"'+param_name+'":[\n'+' '*12+'"'+ret+'"\n'+' '*8+']'
        return(ret)
    
    def transform_multi_params(params):
        params_split = params.split("\n")
        ret = transform_single_param(params_split[0])
        if len(params_split[0])>1:
            for i in range(1,len(params_split)):
                x = transform_single_param(params_split[i])
                ret = ret+',\n'+x
        return(ret)
        
    if output_path == "":
        output_path = input_path.split(".")[0]+"_output.csv" 
        
    data = pd.read_csv(input_path)
    n = data.iloc[:,0].size
    json = ["0"]*n
    for i in range(n):
        event_name = data["event"][i]
        params = data["params"][i]
        json[i] = '{\n'+' '*4+'"'+event_name+'":{\n'+transform_multi_params(params)+"\n"+" "*4+"}\n}"
    json = pd.DataFrame({"json":json})
    json = pd.concat([data,json],axis=1)
    json.to_csv(output_path,index=False)



## Part3: data整形
   


def Get_Type(x): ## x可以是字符串、整数、浮点数或其构成的列表
        type_dict = {"string":str,"int":int,"float":float}
        if x in ["string","int","float"]:
            return(type_dict[x])
        elif type(x) != list:
            return(type(x))
        else:
            return(type(x[0]))

            
def Combine_Str_List(x,y):
    
    if x == []:
        return(y)
    elif y == []:
        return(x)
    elif Get_Type(x) != Get_Type(y):
        return("string")
    elif x in ["string","int","float"]:  
        return(x)
    elif y in ["string","int","float"]:
        return(y)
    elif type(x) == list and type(y) == list:
        return(list(set(x+y)))
    elif type(x) == list:
        x.append(y)
        return(list(set(x)))
    elif type(y) == list:
        y.append(x)
        return(list(set(y)))
    elif x == y:
        return(x)
    else:
        return(list(set([x,y])))


def Combine_Str(series):
        series = list(set(series))
        n = len(series)
        
        if n == 0:
            return("none")
        elif n == 1:
            return(series[0])
        elif n == 2:
            return(series[0]+"或"+series[1])
        else:
            ret = "、".join(series[0:(n-1)])
            ret = ret+"或"+series[n-1] 
            return(ret)


def Transform_Data(input_path):
    
    def get_eventname(json_str):
        json_str = json.loads(json_str)
        return(list(json_str.keys())[0])
        
        
    def get_param(json_str):
        json_str = json.loads(json_str)
        return(list(json_str.values())[0])
    
    
    def combine_json(series):
        n = len(series)
        series.index = range(n)
        json_str = json.loads(series[0])
        if n == 1:
            return(str(json_str))
        else:
            event_name = list(json_str.keys())[0]
            param = list(json_str.values())[0]
            param_name = list(param.keys())
            for i in range(1,len(series)):
                param_add = list(json.loads(series[i]).values())[0]
                for j in param_add.keys():
                    if j in param_name:
                        param[j] = Combine_Str_List(param[j],param_add[j])
                    else:
                        param_name.append(j)
                        param[j] = param_add[j]
            return(str({event_name:param}))
    
    
    def combine_note(series):
        
        def transform_note(note):
            if type(note) != str:
                return({})
            else:
                note = note.split("\n")
                ret = {}
                def trans(x):
                    x = x.split(":")
                    if len(x)==2:
                        return({x[0]:x[1]})
                    else:
                        return({})
                for i in note:
                    d = trans(i)
                    ret.update(d)
                return(ret)
            
        series = list(series.apply(transform_note))
        n = len(series)
        if n == 1:
            return(str(series[0]))
        else:
            ret = series[0]
            param_name = list(ret.keys())
            for i in range(1,n):
                ret_add = series[i]
                param_add = list(ret_add.keys())
                if len(param_add) > 0:
                    for j in param_add:
                        if j in param_name:
                            ret[j] = Combine_Str(pd.Series([ret[j],ret_add[j]]))
                        else:
                            ret[j] = ret_add[j]
                            param_name.append(j)
            return(str(ret))
            
    
    data = pd.read_csv(input_path)
    
    #for i in range(len(data["event"])):
    #    try:
    #        get_eventname(list(data["event"])[i])
    #    except Exception:
    #        print(i)
    
    group = data.groupby(data["event"].apply(get_eventname))
    transform_data = group.agg({"demand":"first",
                            "description":Combine_Str,
                            "event":combine_json,
                            "note":combine_note,
                            "ios_rd":"first",
                            "android_rd":"first",
                            "qa":"first",
                            "pm":"first",
                            "da":"first"})
    return(transform_data)

   
def Data_Check(transform_data,param_exist,module_exist):
    
    L = transform_data["demand"]
    module_list = set(L)
    
    L = transform_data["event"]
    L = list(map(eval,list(L)))
    param_list = []
    for i in L:
        param_list = param_list+list(list(i.values())[0].keys())
    param_list = set(param_list)
    
    param_add = list(param_list - param_exist)
    module_add = list(module_list - module_exist)
    
    type_dict = {int:"integer",float:"float",str:"string"}
    
    def trans(value):
        if type(value) != list:
            return(str(value))
        else:
            return(",".join(map(str,value)))
            
    if len(param_add) > 0:
        note = transform_data["note"]
        type_1 = []
        desc_1 = []
        value_1 = []
        restrict_1 = []
        for i in param_add:
            Value0 = []
            for j in L:
                value = list(j.values())[0]
                if i in value:
                    Value0 = Combine_Str_List(Value0,value[i])     
            type_1.append(type_dict[Get_Type(Value0)])
            
            Value = []
            k1 = 0
            k2 = 0
            for j in note:
                j = eval(j)
                if i in j:
                    value = j[i]
                    if value.endswith('（限制值域）'):
                        value = value.strip('（限制值域）')
                        Value.append(value)
                        k1 = 1-k2
                    else:
                        Value.append(value)
                        k2 = 1
                       
            desc_1.append(Combine_Str(Value))
            restrict_1.append(k1)
            
            if k1 == 1:
                value_1.append(trans(Value0))
                str(Value)
            else:
                value_1.append("none")
        
        df_param = pd.DataFrame({"param":param_add,"type":type_1,"desc":desc_1,"value":value_1,"restrict":restrict_1})
        ret = {"module":module_add,"param":df_param}
        print("以下参数将被添加，请核对相关信息：")
        print(df_param)
        
        ind_pass = 1
        L = list(df_param[df_param["desc"]=="none"]["param"])
        if len(L)>0:
            ind_pass = 0
            print("注意！以下参数未找到参数含义描述，请再次确认。")
            print(L)
        
        L = list(df_param[(df_param["restrict"]==1) & (df_param["value"]=="none")]["param"])
        if len(L)>0:
            ind_pass = 0
            print("注意！以下参数无法对值域进行限制，请再次确认。")
            print(L)
        
        ret.update({"pass":ind_pass})
        if ind_pass == 1:
            print("参数输入信息通过检验。")
        
    else:
        ret = {"module":module_add,"param":None,"pass":1}
        print("无新的参数需要添加。")
        print("参数输入信息通过检验。")

    if len(module_add)>0:
        print("以下需求模块将被添加：")
        print(module_add)
    else:
        print("无新的需求模块需要添加。")
    
    return(ret)
    


#### Part2: Upload into the ET platform.


##


xpath_dict = {"demand":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[7]/div[1]/div/div[2]/div/div/div/div',
              "setting":'//*[@id="root"]/div/div/div[1]/ul/li[4]',
              "setting_demand":'//*[@id="root"]/div/div/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div/div[4]',
              "new_demand":'//*[@id="root"]/div/div/div[2]/div[2]/div/div/div[1]/div[1]/div/button',
              "confirm_demand":'/html/body/div[2]/div/div[2]/div/div[1]/div[2]/form/div[3]/button[2]',
              "new_param_1":'//*[@id="root"]/div/div/div[2]/div[2]/div/div/div[1]/div[1]/div/button',
              "param_type":'/html/body/div[2]/div/div[2]/div/div[1]/div[2]/form/div[2]/div/div[2]/div/div[3]/div/div/div/div/div/div',
              "confirm_param":'/html/body/div[2]/div/div[2]/div/div[1]/div[2]/form/div[6]/button[2]',
              "platform":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[2]/div[2]/div/div/div/div/div[2]',
              "pm":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[9]/div[1]/div/div[2]/div/div/div/div',
              "10":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[10]/div[1]/div/div[2]/div/div/div/div',
              "11":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[11]/div[1]/div/div[2]/div/div/div/div',
              "12":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[12]/div[1]/div/div[2]/div/div/div/div',
              "13":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[13]/div[1]/div/div[2]/div/div/div/div',
              "event_type":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[5]/div[2]/div/div/div/div',
              "module":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[6]/div[1]/div/div[2]/div/div/div/div',
              "select_list":'/html/body/div[%d]/div/div/div/ul/li[%d]',
              "restrict":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[%d]/div[4]/div/div/div[2]/div/p/a',
              "new_value":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[%d]/div[4]/div[1]/div/div[2]/div/div[2]/div/div[2]/a',
              "clear":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[%d]/div[4]/div[2]/a',
              "value_list":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[%d]/div[4]/div[1]/div/div[2]/div/div[2]/div/div[1]/div/div/div/div',
              "param_span":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[%d]/div[1]/div[1]/div/div[2]/div/div[1]/div/div/div/div/div/span[1]',
              "new_param_2":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[%d]/div[1]/div[2]/div[4]',
              "huoshan_click":'//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div[1]',
              "huoshan_select":'/html/body/div[2]/div/div/div/ul/li[7]',
              "new_event":'//*[@id="main-container"]/div[2]/div/div[1]/span/div[2]/a/button',
              "event_submit":'//*[@id="main-container"]/div[2]/div[2]/div[2]/form/div[%d]/div/div/div/span/button'}




def Initial():
    global driver          
    driver = webdriver.Chrome()  
    driver.maximize_window()
    driver.get("http://et.bytedance.net")
    
    print("扫描二维码登录后，按回车继续。")
    os.system("pause")
    
    ##切换到火山
    time.sleep(2)
    driver.find_element_by_xpath(xpath_dict["huoshan_click"]).click()
    driver.find_element_by_xpath(xpath_dict["huoshan_select"]).click()
    time.sleep(2)
    ##新增埋点
    driver.find_element_by_xpath(xpath_dict["new_event"]).click()
    


## input event_name,description,rd,pm,qa,da

def find_by_xpath(x0):
    element = WebDriverWait(driver,10).until(lambda x: x.find_element_by_xpath(x0))
    return(element)
    
def find_by_id(x0):
    element = WebDriverWait(driver,10).until(lambda x: x.find_element_by_id(x0))
    return(element)





def Get_Demand_Param():
    
    driver.refresh()
    time.sleep(3)
    ele = find_by_xpath(xpath_dict["demand"])
    ele.click()
    time.sleep(1)
    
    text = driver.page_source
    ele = find_by_id("demandModuleId")
    ele.send_keys(Keys.ENTER)
    text = BeautifulSoup(text)
    text = text.find_all("li",class_="ant-select-dropdown-menu-item")
    

    demand = []
    for i in text:
        demand.append(i.text)
    
    find_by_id("paramValue0").click()
    time.sleep(1)
    
    text = driver.page_source
    text = BeautifulSoup(text)
    text = text.find_all("li",class_="ant-select-dropdown-menu-item")

    param = []
    for i in text:
        param.append(i.text)
    
    param = param[len(demand):]
    ret = {"demand":demand,"param":param}
    
    return(ret)
        
    



def Add_Module(module_list,method = "semi_auto"):
    ##进入埋点设置页面
    find_by_xpath(xpath_dict["setting"]).click()
    time.sleep(3)
    ##进入需求管理页面
    find_by_xpath(xpath_dict['setting_demand']).click()
    time.sleep(1)
    
    ##新增需求模块
    for i in module_list:
        find_by_xpath(xpath_dict['new_demand']).click()
        ele = find_by_id('name')
        ele.send_keys(i)
        time.sleep(1)
        if method == "semi_auto":
            print("点击确认后，按回车继续。")
            os.system("pause")
            time.sleep(2)
        else:
            find_by_xpath(xpath_dict["confirm_demand"]).click()
            time.sleep(3)
        
    driver.back()



def Add_Param(param_df,method = "semi_auto"):
    n = param_df.iloc[:,0].size
    
    path_dict = {"integer":'/html/body/div[3]/div/div/div/ul/li[1]',
                 "string":'/html/body/div[3]/div/div/div/ul/li[2]',
                 "float":'/html/body/div[3]/div/div/div/ul/li[3]'}
    
    ##进入埋点设置页面
    find_by_xpath(xpath_dict["setting"]).click()
    time.sleep(3)
    ##增加参数
    
    for i in range(n):
        find_by_xpath(xpath_dict["new_param_1"]).click()
        
        find_by_id("name").send_keys(param_df['param'][i])
        find_by_id("descr").send_keys(param_df['desc'][i])
    
        find_by_xpath(xpath_dict["param_type"]).click()
        find_by_xpath(path_dict[param_df['type'][i]]).click()
       
        if param_df['type'][i]=="string" and param_df['restrict'][i]==1:
            find_by_id("value").send_keys(param_df['value'][i])
        
        if method == "semi_auto":
            print("点击确认后，按回车继续。")
            os.system("pause")
            time.sleep(2)
        else:
            find_by_xpath(xpath_dict["confirm_param"]).click()
            time.sleep(3)
    
    driver.back()
    



def Input_Info(data,i,demand_list):
    
    driver.refresh()
    time.sleep(2)
## select platform and input rd,qa,da
    x = data["android_rd"][i]
    y = data["ios_rd"][i]
    
    if x != "none" and y != "none":
        ##ios rd
        find_by_xpath(xpath_dict["10"]).click()
        find_by_id("iosEmailPrefix").send_keys(y)
        ##android rd
        find_by_xpath(xpath_dict["11"]).click()
        find_by_id("androidEmailPrefix").send_keys(x)
        ##qa
        find_by_xpath(xpath_dict["12"]).click()
        find_by_id("qaEmailPrefix").send_keys(data["qa"][i])
        ##da
        find_by_xpath(xpath_dict["13"]).click()
        find_by_id("daEmailPrefix").send_keys(data["da"][i])
      
    elif x != "none":
        ##platform
        xpath = '/html/body/div[2]/div/div/div/ul/li[2]'
        find_by_xpath(xpath_dict["platform"]).click()
        time.sleep(2)
        find_by_xpath(xpath).click()
        ##android rd
        find_by_xpath(xpath_dict["10"]).click()
        find_by_id("androidEmailPrefix").send_keys(x)
        ##qa
        find_by_xpath(xpath_dict["11"]).click()
        find_by_id("qaEmailPrefix").send_keys(data["qa"][i])
        ##da
        find_by_xpath(xpath_dict["12"]).click()
        find_by_id("daEmailPrefix").send_keys(data["da"][i])
        
    else:
        ##platform
        xpath = '/html/body/div[2]/div/div/div/ul/li[3]'
        find_by_xpath(xpath_dict["platform"]).click()
        time.sleep(2)
        find_by_xpath(xpath).click()
        ##ios rd
        find_by_xpath(xpath_dict["10"]).click()
        find_by_id("iosEmailPrefix").send_keys(y)
        ##qa
        find_by_xpath(xpath_dict["11"]).click()
        find_by_id("qaEmailPrefix").send_keys(data["qa"][i])
        ##da
        find_by_xpath(xpath_dict["12"]).click()
        find_by_id("daEmailPrefix").send_keys(data["da"][i])
    
    
    ## input event_name and event_description
    ele = find_by_id('name')
    ele.clear()
    ele.send_keys(list(eval(data["event"][i]).keys())[0])
    
    ele = find_by_id("descr")
    ele.clear()
    ele.send_keys(data["description"][i])
    
    ## input pm
    find_by_xpath(xpath_dict["pm"]).click()
    find_by_id("pmEmailPrefix").send_keys(data["pm"][i])
    
    ## input demand
    ind = demand_list.index(data["demand"][i])+1
    find_by_xpath(xpath_dict["demand"]).click()
    find_by_xpath(xpath_dict["select_list"] %(7,ind)).click()
    
        


def Select_Module_Type(params):
    
    if params["event_type"] == "core":
        x = "核心埋点"
    else:
        x = "常规埋点"
    find_by_xpath(xpath_dict["event_type"]).click()
    ele = find_by_id("typeId")
    ele.send_keys(x)
    ele.send_keys(Keys.ENTER)
    
    if params["event_belong"] == "video":
        x = "火山小视频"
    elif params["event_belong"] == "live":
        x = "火山直播"
    else:
        x = "默许模块"
    find_by_xpath(xpath_dict["module"]).click()
    ele = find_by_id("moduleId")
    ele.send_keys(x)
    ele.send_keys(Keys.ENTER)



def Input_Param(params,param_list,method = "semi_auto"):
    
    global K,N
    
    K = 10
    N = len(params)
    key = list(params.keys())
    value = list(params.values())
    
    
    def transform_value(v):
        if type(v) != list:
            return(str(v))
        else:
            vv = map(str,v)
            vv = ','.join(vv)
            return(vv) 
   
    def input_param(i,method,param_list):
        
        global K
        ind = param_list.index(key[i])+1
        time.sleep(1)
        find_by_id('paramValue'+str(i)).click()
        time.sleep(1)
        find_by_xpath(xpath_dict["select_list"] %(K,ind)).click()
        K0 = K
        K += 1
        time.sleep(1)
        
        if value[i] in ["float","int","string"]:
            ele = driver.find_elements_by_xpath(xpath_dict["new_value"] % (8+i))
            if len(ele)>0:
                0/0
        else:
            ele = driver.find_elements_by_xpath(xpath_dict["clear"] % (8+i))
            if len(ele)>0:
                ##获取值域
                ele = find_by_xpath(xpath_dict["value_list"] % (i+8))
                text = ele.text
                text = text.split("\n")
                
                if type(value[i]) != list:
                    v = [value[i]]
                else:
                    v = value[i]
                
                x = list(set(v)-set(text))
                
                ##添加值域
                if len(x)>0:
                    find_by_xpath(xpath_dict["new_value"] % (8+i)).click()
                    win = driver.window_handles
                    driver.switch_to_window(win[1])
                    time.sleep(3)
                    ele = find_by_id("addParamValue")
                    ele.send_keys(transform_value(x))
                    time.sleep(2)
                    if method == "semi_auto":
                        print("点击确认后，按回车继续。")
                        os.system("pause")
                    else:
                        find_by_xpath(xpath_dict["confirm_param"]).click()
                    
                    driver.close()
                    ##回到原来的窗口
                    driver.switch_to_window(win[0])
                    ##重新输入参数
                    find_by_xpath(xpath_dict["param_span"] % (8+i)).click()
                    find_by_id('paramValue'+str(i)).click()
                    find_by_xpath(xpath_dict["select_list"] %(K0,ind)).click()
                    time.sleep(1)
                    ##重新获取值域
                    ele = find_by_xpath(xpath_dict["value_list"] % (i+8))
                    text = ele.text
                    text = text.split("\n")
                
                
                find_by_xpath(xpath_dict["clear"] % (8+i)).click()
                time.sleep(1)
                find_by_xpath(xpath_dict["value_list"] % (i+8)).click()
                time.sleep(1)
                
                for j in range(len(text)):
                    if text[j] in v:
                        find_by_xpath(xpath_dict["select_list"] % (K,(j+1))).click()
                K += 1
                
            else:
                find_by_xpath(xpath_dict["restrict"] % (8+i)).click()
                ele = find_by_id("paramValue%dEnum" % i)
                ele.send_keys(transform_value(value[i]))
                
    input_param(0,method,param_list)    
    if N>1:
        for ii in range(1,N):
            find_by_xpath(xpath_dict["new_param_2"] % (ii+7)).click()
            time.sleep(1)
            input_param(ii,method,param_list)
    



def Input_Single_Event(data,m,demand_list,param_list,method = "semi_auto"):
    params = list(eval(data["event"][m]).values())[0]
    Input_Info(data,m,demand_list)
    Select_Module_Type(params)
    Input_Param(params,param_list)
    if method == "semi_auto":
        print("核对无误点击确认后，按回车键继续。")
        os.system("pause")
    else:
        find_by_xpath(xpath_dict["event_submit"] % (N+13)).click()




def Input_Multiple_Event(input_path,method="semi_auto"):
    data = Transform_Data(input_path)
    
    Initial()
    time.sleep(2)
    ret = Get_Demand_Param()
    param_exist = set(ret["param"])
    module_exist = set(ret["demand"])
    data2 = Data_Check(data,param_exist,module_exist)
    
    if data2["pass"] == 1:
        module_list = data2["module"]
        param_df = data2["param"]
        
        x = 0
        if len(module_list) >0:
            Add_Module(module_list,method)
            x = 1
        if param_df != None:
            Add_Param(param_df,method)
            x = 1
        
        if x == 1:
            ret = Get_Demand_Param()
            
        param_list = ret["param"]
        demand_list = ret["demand"]
        
        FAIL = 0
        SUC = 0
        for m in range(data.iloc[:,0].size):
            fail = 0
            suc = 0
            event_name = list(eval(data["event"][m]).keys())[0]
            while fail <= 1 and suc == 0:
                try:
                    Input_Single_Event(data,m,demand_list,param_list,method)
                    suc = 1
                    SUC = SUC+1
                except Exception:
                    fail = fail+1
                    if fail == 2:
                        FAIL = FAIL+1
                        print("事件%s录入失败，请手动检查后录入。" % event_name)
        print("本次埋点录入完毕，共计%d个埋点录入成功，%d个埋点录入失败。" % (SUC,FAIL))
        driver.quit()
    else:
        print("请根据提示重新确认输入数据。")
        driver.quit()
    