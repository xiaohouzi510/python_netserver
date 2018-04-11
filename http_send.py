#!/usr/bin/python
# encoding=utf-8

import re
import url
import http_internal

#---http 发送数据
class http_send_data(http_internal.http_data):
	def __init__(self,host,content,fun_cb,url,method,header):
		http_internal.http_data.__init__(self)
		self.m_host    = host
		self.m_content = content
		self.m_fun_cb  = fun_cb
		self.m_url 	   = url 
		self.m_method  = method
		self.m_header  = header

#------http 发送---------
class http_send:
	def __init__(self,parts_mgr):
		self.m_parts_mgr = parts_mgr
		self.m_send_data = {}
		self.m_check_ver = False 

	#连接成功，回调
	def connect_cb(self,sock_msg):
		if not self.m_send_data.has_key(sock_msg.m_iFd):
			print("http connect cb not found fd=%d"%(sock_msg.m_iFd))
			return
		s_data = self.m_send_data[sock_msg.m_iFd]
		self.send_http_data(sock_msg.m_iFd,s_data)

	#接收到数据回调
	def recv_data_cb(self,sock_msg,ud):
		code = http_internal.deal_http_data(self,self.m_send_data[sock_msg.m_iFd],sock_msg.m_sData)
		if code != 0:
			self.m_parts_mgr.m_stNetServer.CloseSock(sock_msg.m_iFd)
			print("response error=%d fd=%d"%(code,sock_msg.m_iFd))

	#fd 关闭回调
	def close_cb(self,fd_data):
		if not self.m_send_data.has_key(fd_data.m_iFd): 
			print("http send close not found fd=%d"%(fd_data.m_iFd))
			return
		del self.m_send_data[fd_data.m_iFd]

	#接收完成
	def recv_finish(self,s_data):
		s_data.m_fun_cb(s_data.m_body,s_data.m_header)
		self.m_parts_mgr.m_stNetServer.CloseSock(s_data.m_fd)

	#发送 get http 数据，content 为字符串类型
	def http_get_data(self,host,content,fun_cb,url="/"):
		self.deal_connect(host,content,fun_cb,url,"GET",{})

	#发送 post http 数据，form 为字典类型
	def http_post_data(self,host,form,fun_cb,url="/"):
		header  = {"content-type":"application/x-www-form-urlencoded"}
		content = []
		for k in form:
			v = form[k]
			content.append("%s=%s"%(k,v))
		self.deal_connect(host,'&'.join(content),fun_cb,url,"POST",header)

	#建立连接
	def deal_connect(self,host,content,fun_cb,url,method,header):
		obj    = re.match("([^:]*):?(\d*)$",host)
		ip     = obj.groups()[0]
		port   = obj.groups()[1] != "" and int(obj.groups()[1]) or 80 
		fd     = self.m_parts_mgr.m_stNetServer.DoConnect(ip,port,self.recv_data_cb,self.connect_cb,self.close_cb)
		s_data = http_send_data(host,content,fun_cb,url,method,header)
		s_data.m_fd    = fd
		s_data.m_statu = http_internal.http_statu.ehttp_head 
		self.m_send_data[fd] = s_data

	#发送 http 数据
	def send_http_data(self,fd,s_data):
		header_content = ""
		if not s_data.m_header.has_key("host"):	
			s_data.m_header["host"] = s_data.m_host
		for k in s_data.m_header:
			header_content = "%s%s:%s\r\n"%(header_content,k,s_data.m_header[k])
		length = len(s_data.m_content)
		request_header = "%s %s HTTP/1.1\r\n%scontent-length:%d\r\n\r\n"%(s_data.m_method,s_data.m_url,header_content,length)
		self.m_parts_mgr.m_stNetServer.SendData(fd,request_header)
		length != 0 and self.m_parts_mgr.m_stNetServer.SendData(fd,s_data.m_content)
		s_data.m_header = {}