#!/usr/bin/python
# encoding=utf-8

import MySQLdb
import datetime
import time
import json
import zlib 
import re
import logging

logger = logging.getLogger(__name__)

class best_consumer:
	def __init__(self):
		self.m_db_config_files 	    = {} 	   #db 配置文件名
		self.m_db_config_jsons 	    = {} 
		self.m_table_describe_files = {} 	   #配表描术文件名
		self.m_table_name_prefixs   = {}	   #表名前缀
		self.m_mysql_field_strs     = {} 	   #mysql 字段(field1,field2,...)
		self.m_mysql_field_jsons    = {} 
		self.m_check_fun 			= None 
		self.m_field_customize_funs = {}
		self.m_db 					= {} 
		self.m_table_type 		    = None

	#添加配置
	def add_config(self,etype,db_file,table_describe,table_prefix):
		if not self.m_table_type.has_key(etype):
			return False,"add config not found type=%s %s %s %s"%(etype,db_file,table_describe,table_prefix)
		self.m_db_config_files[etype]	   = db_file
		self.m_table_describe_files[etype] = table_describe
		self.m_table_name_prefixs[etype]   = table_prefix
		return True,"OK"

	#添加 field 自定义获取参数，如：系统当前时间的域
	def add_field_customize_fun(self,etype,key,fun):
		if not self.m_table_type.has_key(etype):
			return False,"add field not found type=%s %s %s"%(etype,key,fun)
		if not self.m_field_customize_funs.has_key(etype):
			self.m_field_customize_funs[etype] = {}
		self.m_field_customize_funs[etype][key] = fun
		return True,"OK"

	#获得日期
	def get_time_statmp(self,field_describe,one_row):
		return "'" + time.strftime("%Y-%m-%d %H:%M:%S") + "'","OK"

	#os 转换
	def os_change(self,field_describe,one_row):
		key = field_describe["key"] 
		if not one_row.has_key(key):
			return False,"not found field=%s"%key
		value_str = one_row[key]
		if value_str == "android":
			return str(1),"OK"
		elif value_str == "windows":
			return str(0),"OK"
		elif value_str == "ios":
			return str(2),"OK"
		return False,"error field=%s value=%s"%(key,value_str)

	#初始化
	def init(self):
		for etype in self.m_table_describe_files:
			file_name = self.m_table_describe_files[etype]
			status,result = self.read_json_file(file_name)
			if not status:
				return status,result
			self.m_mysql_field_jsons[etype] = result
			self.init_mysql_field(etype,result)
			status,result = self.read_json_file(self.m_db_config_files[etype])
			if not status:
				return False,"read db config error=%s code=%s file=%s"%(status,result,self.m_db_config_files[etype])
			status,db = self.connect_db(result)
			if not status:
				return status,db
			self.m_db[etype] = db
			self.m_db_config_jsons[etype] = result
		return True,"OK"

	#初始化表
	def init_table(self):
		#各表类型
		self.m_table_type = {
			"step_point" : "step_point",
			"device"     : "device"
		}

	#连接数据库
	def connect_db(self,json_array):
		try:
			host = json_array["host"]
			user = json_array["username"]
			port = json_array["port"]
			pwd  = json_array["pwd"]
			db   = json_array["dbname"]
			obj  = MySQLdb.connect(host=host,user=user,port=port,passwd=pwd,db=db)
		except Exception,info:
			return False,info 
		return True,obj

	#执行语句
	def execute(self,etype,sql):
		try:
			self.m_db[etype].ping()
		except Exception,info:
			logger.warning("db disconnect =%s"%self.m_db[etype])
		try:
			self.m_db[etype].cursor().execute(sql)
		except Exception,info:
			return False,"execute error=%s db=%s"%(info,self.m_db[etype])
		return True,"OK"

	#初始化 mysql 字段表，格式 see：best_device_table_describe.json 文件
	def init_mysql_field(self,etype,mysql_json_array):
		reslut = []
		for k,field in enumerate(mysql_json_array):
			reslut.append(field["key"])
		self.m_mysql_field_strs[etype] = "(%s)" %(",".join(reslut))

	#读 json 文件
	def read_json_file(self,file_name):
		try:
			file = open(file_name,"r")	
			s_str = file.read()
			file.close()
		except Exception,info:
			return False,info 
		return self.json_decode(s_str)

	#将字符串转成 json
	def json_decode(self,json_str):
		try:
			s_str = json.loads(json_str)
		except Exception,info:
			return False,"json_decode error=%s"%info	
		return True,s_str

	#字符串检测
	def str_check(self,field_describe,field_value):
		key   = field_describe["key"] 
		i_len = field_describe["len"]
		try:
			result = str(field_value)
		except Exception,info:
			return False,"field=%s not string value=%s"%(key,field_value)
		if len(field_value) > i_len:
			return False,"field=%s str=%d large than %d"%(key,len(field_value),i_len)
		return "'%s'"%result,"OK"

	#整数类型检测
	def int_check(self,field_describe,field_value):
		try:
			result = int(field_value,10)
		except Exception,info:
			return False,"field=%s not a number error=%s"%(field_describe["key"],info)
		return str(result),"OK"

	#检查一个 field 是否合法
	def check_field(self,etype,field_describe,one_row):
		key   	   = field_describe["key"]
		field_type = field_describe["type"]
		if self.m_field_customize_funs.has_key(etype) and self.m_field_customize_funs[etype].has_key(key):
			return self.m_field_customize_funs[etype][key](field_describe,one_row)
		if not one_row.has_key(key):
			return False,"not found field=%s"%(key)
		e_type = field_type.lower()
		if not self.m_check_fun.has_key(e_type):
			return False,"not found check fun type =%s field=%s"%(e_type,key)
		return self.m_check_fun[e_type](field_describe,one_row[key])

	#初始化检测函数
	def init_check(self):
		self.m_check_fun = {
			"varchar" : self.str_check,
			"char"    : self.str_check,
			"int"     : self.int_check,
			"tinyint" : self.int_check,
		}

	#获得 mysql insert value 
	def get_mysql_insert_value(self,json_data_array):
		temp = {} 
		for k,one_row in enumerate(json_data_array):
			data      = one_row["data"]
			etype     = one_row["type"]
			one_value = []
			for k,field in enumerate(self.m_mysql_field_jsons[etype]):
				value,str_error = self.check_field(etype,field,data)
				if value:
					one_value.append(value)
				else:
					return value,str_error
			if not temp.has_key(etype):
				temp[etype] = []
			temp[etype].append("(%s)"%(",".join(one_value)))
		result = {}
		for k in temp:
			v = temp[k]
			result[k] = ",".join(v)
		return True,result

	#公共检测(该函数后继如果不通用，可修改)
	def check_common(self,json_data_array): 
		result = []
		for k,v in enumerate(json_data_array):
			status,json = self.json_decode(v)
			if not status:
				return status,json
			if not json.has_key("type"):
				return False,"check key error [type] key not found"
			if not json.has_key("data"):
				return False,"check key error [data] key not found"
			if not self.m_table_type.has_key(json["type"]):
				return False,"check common not found type=%s"%json.type
			result.append(json)
		return True,result

	#获得表名
	def get_tablename(self,etype):
		return "%s%s"%(self.m_table_name_prefixs[etype],time.strftime("%Y%m%d"))

	#解压数据
	def zip_inflate(self,json_str):
		try:
			result = zlib.decompress(json_str)
		except Exception,info:
			return False,"zip_inflate error=%s"%info
		return True,result

	#分割字符串
	def split_str(self,json_str): 
		pattern = re.compile("[^\r\n]+")
		array = pattern.findall(json_str) 
		return array

	#run 函数
	def run(self,json_str):
		status,json_str = self.zip_inflate(json_str)
		if not status:
			return status,json_str
		json_str_array = self.split_str(json_str)
		#一份数据允许有多个 type
		b_error,data = self.check_common(json_str_array)
		if not b_error:
			return b_error,data 
		b_error,insert_value = self.get_mysql_insert_value(data)
		if not b_error:
			return False,"error get insert value b_error=%s code = %s"%(b_error,insert_value)
		for etype in insert_value:
			value = insert_value[etype]
			insert_sql = "insert into %s %s values%s;"%(self.get_tablename(etype),self.m_mysql_field_strs[etype],value)
			status,error_str = self.execute(etype,insert_sql)
			if status:
				self.m_db[etype].commit()
				continue
			return status,error_str
		return True,"OK"