#!/usr/bin/python
# encoding=utf-8

import http_internal
import url
from inspect import isfunction

http_status_msg = {
	100:"Continue",
	101:"Switching Protocols",
	200:"OK",
	201:"Created",
	202:"Accepted",
	203:"Non-Authoritative Information",
	204:"No Content",
	205:"Reset Content",
	206:"Partial Content",
	300:"Multiple Choices",
	301:"Moved Permanently",
	302:"Found",
	303:"See Other",
	304:"Not Modified",
	305:"Use Proxy",
	307:"Temporary Redirect",
	400:"Bad Request",
	401:"Unauthorized",
	402:"Payment Required",
	403:"Forbidden",
	404:"Not Found",
	405:"Method Not Allowed",
	406:"Not Acceptable",
	407:"Proxy Authentication Required",
	408:"Request Time-out",
	409:"Conflict",
	410:"Gone",
	411:"Length Required",
	412:"Precondition Failed",
	413:"Request Entity Too Large",
	414:"Request-URI Too Large",
	415:"Unsupported Media Type",
	416:"Requested range not satisfiable",
	417:"Expectation Failed",
	500:"Internal Server Error",
	501:"Not Implemented",
	502:"Bad Gateway",
	503:"Service Unavailable",
	504:"Gateway Time-out",
	505:"HTTP Version not supported",
}

#---------------http 模块--------------
class http_recv:
	def __init__(self,parts_mgr,cb_fun):
		self.m_parts_mgr = parts_mgr
		self.m_http_data = {}   		#所有正在处理的 http 客户端数据
		self.m_cb_fun 	 = cb_fun
		self.m_check_ver = True

	#开始
	def start_server(self,ip,port):
		self.m_parts_mgr.m_stNetServer.DoListen(ip,port,self.recv_http_data_cb,self.accept_http_cb,self.close_cb)	

	#http 收到数据
	def recv_http_data_cb(self,sock_msg,ud):
		h_data = self.m_http_data[sock_msg.m_iFd]
		code   = http_internal.deal_http_data(self,h_data,sock_msg.m_sData)
		#有错误码情况
		if code != 0:
			h_data.m_statu = http_internal.http_statu.ehttp_none
			self.http_response(sock_msg.m_iFd,None,code)

	#接收完成
	def recv_finish(self,h_data):
		self.m_cb_fun(h_data.m_body,h_data.m_addr,h_data.m_fd,h_data.m_header)

	#fd 关闭回调
	def close_cb(self,fd_data):
		if not self.m_http_data.has_key(fd_data.m_iFd):
			print("http close not found fd=%d"%(fd_data.m_iFd))
			return
		del self.m_http_data[fd_data.m_iFd]

	#http 返回
	def http_response(self,fd,body,code=200):
		if not self.m_http_data.has_key(fd):
			print("http response not found fd=%d"%(fd))
			return
		#状态行
		status_line = "HTTP/1.1 %03d %s\r\n"%(code,http_status_msg[code] or "")
		self.m_parts_mgr.m_stNetServer.SendData(fd,status_line)
		if type(body) == type(""):
			self.m_parts_mgr.m_stNetServer.SendData(fd,"content-length: %d\r\n\r\n"%(len(body)))
			self.m_parts_mgr.m_stNetServer.SendData(fd,body)
		elif isfunction(body):
			self.m_parts_mgr.m_stNetServer.SendData(fd,"transfer-encoding: chunked\r\n")
			while True:	
				s_str = body()
				if s_str == None:
					self.m_parts_mgr.m_stNetServer.SendData(fd,"\r\n0\r\n\r\n")
					break
				self.m_parts_mgr.m_stNetServer.SendData(fd,"\r\n%x\r\n"%(len(s_str)))
				self.m_parts_mgr.m_stNetServer.SendData(fd,s_str)
		else:
			self.m_parts_mgr.m_stNetServer.SendData(fd,"\r\n")
		self.m_parts_mgr.m_stNetServer.CloseSock(fd)

	#接收到新的 fd，addr 为元组(ip,port)
	def accept_http_cb(self,fd_data,addr):
		if self.m_http_data.has_key(fd_data.m_iFd):
			print("http accept double fd=%d ip=%s port=%d"%(fd_data.m_iFd,addr[0],addr[1]))
			return
		h_data = http_internal.http_data()
		h_data.m_fd    = fd_data.m_iFd
		h_data.m_addr  = addr
		h_data.m_statu = http_internal.http_statu.ehttp_head
		self.m_http_data[fd_data.m_iFd] = h_data