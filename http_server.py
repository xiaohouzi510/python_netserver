#!/usr/bin/python
# encoding=utf-8

import time
import MsgQueue
import NetServer
import TimeMgr
import SocketServer
import NetPack
import NetQueue
import http_recv
import logging
import logging.handlers
import url

#日志文件名
_DEBUG_FILE = './runlog.log'
_ERROR_FILE = './error_runlog.log'

def ConfigLog():
	formatter = logging.Formatter("[%(asctime)-11s] [%(thread)d] [%(levelname)s] [%(filename)s:%(lineno)s] %(message)s")
	handler = logging.handlers.TimedRotatingFileHandler(_DEBUG_FILE, when='D', backupCount=10)
	error_handler = logging.handlers.TimedRotatingFileHandler(_ERROR_FILE, when='W6', backupCount=30)
	level = 10
	logging.getLogger('').setLevel(level)

	handler.setLevel(level)
	error_handler.setLevel(logging.ERROR)

	handler.setFormatter(formatter)
	error_handler.setFormatter(formatter)

	logging.getLogger('').addHandler(handler)
	logging.getLogger('').addHandler(error_handler)

#获取毫秒
def GetMillisecond():
	return int(time.time()*1000)

#一个线程所有部件,该线程只操作这些变量,隔离于其它线程
class PartsMgr:
	def __init__(self,stThread):
		self.m_stMsgQ        = None 
		self.m_stTimerMgr    = None 
		self.m_stSockServer  = None 
		self.m_stNetServer   = None 
		self.m_stThread      = stThread

#工作线程类
class WorkThread():
	def __init__(self):
		self.m_stPartsMgr = PartsMgr(self)
		self._iThreadId   = 1

	#一次循环
	def OneLoop(self):
		iMaxInterval  = 1000
		iCurTime      = GetMillisecond()
		bSleep        = self.m_stPartsMgr.m_stMsgQ.MsgLoop()
		iInterval     = GetMillisecond() - iCurTime
		bSleep        = not bSleep
		if iInterval > iMaxInterval:
			print("thread time out diffTime:%d"%(iInterval))
		return bSleep

	#获取线程 id
	def GetThreadId(self):
		return self._iThreadId

#http 回调函数
def http_get_data():
	global g_index
	g_index += 1
	if g_index == 1:
		return "name=libinbin&"
	elif g_index == 2:
		return "id=10086&"
	elif g_index == 3:
		return "sex=female"
	g_index = 0
	return None

#http 接收到数据
def http_recv_package(body,addr,fd):
	print("fd=%d recv=[%s]"%(fd,url.parse_query(body)))
	g_recv_mgr.http_response(fd,body)

g_index    = 0
g_body     = None
g_recv_mgr = None 
g_stWork   = WorkThread()

#主函数
if __name__ == "__main__":
	ConfigLog()
	#消息调度模块
	g_stWork.m_stPartsMgr.m_stMsgQ    	  = MsgQueue.MsgQueue(g_stWork.m_stPartsMgr)
	#定时器模块
	g_stWork.m_stPartsMgr.m_stTimerMgr    = TimeMgr.WheelTimerMgr()
	#底层网络服务
	g_stWork.m_stPartsMgr.m_stSockServer  = SocketServer.SocketServer()
	#逻辑网络服务
	g_stWork.m_stPartsMgr.m_stNetServer   = NetServer.NetServer(g_stWork.m_stPartsMgr)
	#http 模块
	g_recv_mgr = http_recv.http_recv(g_stWork.m_stPartsMgr,http_recv_package)
	#开始 http 服务
	g_recv_mgr.start_server("0.0.0.0",8889)
	#设置 epoll_wait 等时间为 0
	g_stWork.m_stPartsMgr.m_stSockServer.SetWaitTime(0)
	while True:
		while g_stWork.m_stPartsMgr.m_stSockServer.ServerPoll():
			i = 1 
		while not g_stWork.OneLoop():
			i = 1
		g_stWork.m_stPartsMgr.m_stTimerMgr.UpdateTime()
		time.sleep(0.01)