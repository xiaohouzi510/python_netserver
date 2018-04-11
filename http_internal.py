#!/usr/bin/python
# encoding=utf-8

import re
import url

http_limit 		 = 8192
body_limit 		 = 8192
chunk_size_limit = 128

#http 状态
class http_statu: 
	ehttp_none  = 0
	ehttp_head  = 1
	echunk_size = 2
	echunk_body = 3
	echunk_tail = 4
	ebody_iden  = 5

#一个 http 数据
class http_data:
	def __init__(self):
		self.m_statu      = http_statu.ehttp_none
		self.m_header     = {} 
		self.m_sz 	      = 0 
		self.m_body       = "" 
		self.m_surplus    = "" 
		self.m_fd 	      = 0
		self.m_addr       = None
		self.m_statu_line = None

#解析 url 参数
def parse_url(s_str):
	start_index = str.find(s_str,"?")
	if start_index == -1:
		return ""
	end_index = str.find(s_str,"HTTP")
	if end_index == -1:
		return "" 
	return s_str[start_index+1:end_index-1]

#读取头部
def recv_header(surplus,recv_data):
	surplus += recv_data
	data_len = len(surplus)
	if data_len >= http_limit:
		print("http data too large size=%d"%data_len)
		return -1,None,None
	if data_len >= 2:
		index = str.find(surplus,"\r\n")	
		if index == 0:
			return 0,None,surplus[2:]
	index = str.find(surplus,"\r\n\r\n")
	if index == -1:
		return -2,None,surplus
	header_str = surplus[0:index+2] 
	pattern = re.compile("(.*)\r\n")
	array = pattern.findall(header_str) 
	return 0,array,surplus[index+4:]

#解析头部
def parse_header(header_array,result_array):
	array_len = len(header_array)
	name,value = None,None
	pattern = re.compile("([^:]+): *(.+)")
	for i in xrange(1,array_len):
		line = header_array[i]
		#tab 键，表明是多行
		if ord(line[0:1]) == 9:
			result_array[name] += line[1:] 
		else:
			pat_result = pattern.findall(line)
			#客户端请求语法出错
			if len(pat_result) == 0:
				return 400
			name  = pat_result[0][0].lower()
			value = pat_result[0][1] 
			if result_array.has_key(name):
				data = result_array[name]
				if data == 'list':
					data.append(value)
				else:
					result_array[name] = [data,value]
			else:
				result_array[name] = value
	return 0

#chunk size
def chunked_size(surplus,recv_data): 
	surplus += recv_data
	index = str.find(surplus,"\r\n")
	#too large
	if len(surplus) > chunk_size_limit:
		return -1,None
	if index == -1:
		return -2,surplus
	sz = int(surplus[:index],16)
	return sz,surplus[index+2:]

#接收 chunked 类型，返回：成功、数据、剩余数据
def recv_chunked_body(sz,surplus,recv_data):
	surplus += recv_data
	index = str.find(surplus,"\r\n")
	#too large
	if len(surplus) >= body_limit:
		return -1,None,None
	#未找到 \r\n
	if index == -1:
		return -2,None,surplus
	return 0,surplus[:index],surplus[index+2:]

#处理 http 数据
def deal_http_data(self,h_data,sData):
	if h_data.m_statu == http_statu.ehttp_head:
		code = deal_head(self,h_data,sData)
	elif h_data.m_statu == http_statu.echunk_size:
		code = deal_chunked_size(self,h_data,sData)
	elif h_data.m_statu == http_statu.echunk_body:
		code = deal_chunked_body(self,h_data,sData)
	elif h_data.m_statu == http_statu.echunk_tail:
		code = deal_chunked_tail(self,h_data,sData)
	elif h_data.m_statu == http_statu.ebody_iden:
		code = deal_iden(self,h_data,sData)
	else:
		code = 0 
		print("recv statu fd=%d"%(h_data.m_fd))
	return code

#处理头部
def deal_head(self,h_data,recv_data):
	code,header_array,surplus = recv_header(h_data.m_surplus,recv_data)
	#too large
	if code == -1:
		return 413
	if header_array != None:
		i_error = parse_header(header_array,h_data.m_header)
		#客户端请求语法出错
		if i_error != 0:
			return i_error
		#保存请求行
		h_data.m_statu_line = header_array[0]
	if surplus != None:
		h_data.m_surplus = surplus
	if code != 0:
		return 0
	mode = None
	if h_data.m_header.has_key("transfer-encoding"):
		mode = h_data.m_header["transfer-encoding"]
	if mode != None and mode != "identity" and mode != "chunked":
		print("http header error")
		return 501
	if mode == "chunked":
		h_data.m_statu = http_statu.echunk_size
	else:
		h_data.m_statu = http_statu.ebody_iden
		if h_data.m_header.has_key("content-length"):
			h_data.m_sz = int(h_data.m_header["content-length"])
	return deal_http_data(self,h_data,"")

#处理 chunked 长度
def deal_chunked_size(self,h_data,recv_data):
	h_data.m_sz,surplus = chunked_size(h_data.m_surplus,recv_data)
	#chunked size too large
	if h_data.m_sz == -1:
		return 413 
	h_data.m_surplus = surplus
	#未找到 \r\n
	if h_data.m_sz < 0:
		return 0
	elif h_data.m_sz > 0:
		h_data.m_statu = http_statu.echunk_body
	elif h_data.m_sz == 0:
		h_data.m_statu = http_statu.echunk_tail
		h_data.m_sz = len(h_data.m_surplus)
	return deal_http_data(self,h_data,"")

#处理 chunked 数据部份
def deal_chunked_body(self,h_data,recv_data):
	code,body,surplus = recv_chunked_body(h_data.m_sz,h_data.m_surplus,recv_data)
	#too large
	if code == -1:
		return 413
	h_data.m_surplus = surplus
	if code != 0:
		return 0
	h_data.m_body += body
	h_data.m_statu = http_statu.echunk_size
	return deal_http_data(self,h_data,"")

#处理 chunked 尾部
def deal_chunked_tail(self,h_data,recv_data):
	code,header_array,surplus = recv_header(h_data.m_surplus,recv_data)
	#too large
	if code == -1:
		return 413
	if header_array != None:
		parse_header(header_array,h_data.m_header)
	if surplus != None:
		h_data.m_surplus = surplus
	if code != 0:
		return 0
	h_data.m_body = url.parse_query(h_data.m_body)
	self.recv_finish(h_data)
	return 0

#处理 identify 数据
def deal_iden(self,h_data,recv_data):
	h_data.m_surplus += recv_data
	if len(h_data.m_surplus) >= body_limit:
		return 413
	if len(h_data.m_surplus) < h_data.m_sz:
		return 0
	h_data.m_body = h_data.m_surplus
	if h_data.m_sz == 0:
		h_data.m_body = parse_url(h_data.m_statu_line) 
	self.recv_finish(h_data)
	return 0