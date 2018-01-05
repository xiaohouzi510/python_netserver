#!/usr/bin/python
# encoding=utf-8

import time
import threading
import MsgQueue
import NetServer
import TimeMgr
import SocketServer
import NetPack
import logging
import struct
import NetQueue
import logging.handlers
import signal
import os
import sys

'''日志文件名'''
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


#一个线程所有部件,该线程只操作这些变量,隔离于其它线程
class PartsMgr:
	def __init__(self):
		self.m_stThread      = None 
		self.m_stMsgQ        = None 
		self.m_stTimerMgr    = None 
		self.m_stSockServer  = None 
		self.m_stNetServer   = None 

#工作线程类
class WorkThread(threading.Thread):
	def __init__(self,iThreadId):
		threading.Thread.__init__(self)
		self.m_stPart     = PartsMgr()
		self.m_iThreadID  = iThreadId
		self.setDaemon(True)

	#获得 ID
	def GetThreadId(self):
		return self.m_iThreadID

	#线程循环
	def run(self):
		while True:
			if not self.m_stPart.m_stMsgQ.MsgLoop():
				time.sleep(0.005)

#网络线程
class NetThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.m_stSockServer = SocketServer.SocketServer()
		self.setDaemon(True)

	#线程循环
	def run(self):
		while True:
			self.m_stSockServer.ServerPoll()

#定时器线程
class TimerThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.m_stTimerMgr = TimeMgr.WheelTimerMgr()
		self.setDaemon(True)

	def run(self):
		while True:
			self.m_stTimerMgr.UpdateTime()
			time.sleep(0.0025)

#-------------------------------------------------
g_hFd = {}

#accept 回调
#param stNetFd = NetServer.NetFdData 
def AcceptCb(stNetFd,stAddr):
	g_hFd[stNetFd.m_iFd] = {"fd":stNetFd.m_iFd,"ip":stAddr[0],"port":stAddr[1]}
	stNetFd.m_stUd   	 = NetPack.PackQueue()
	print("accept one socket=%d ip=%s port=%d"%(stNetFd.m_iFd,stAddr[0],stAddr[1]))

#关闭 socket 回调
def CloseCb(stNetFd):
	stFd = g_hFd[stNetFd.m_iFd]
	del g_hFd[stNetFd.m_iFd]
	print("close one socket=%d ip=%s port=%d"%(stNetFd.m_iFd,stFd["ip"],stFd["port"]))

#收到数据回调
#param stSockMsg = SocketServer.SockMessage 
#param stUd = NetPack.PackQueue 
def DataCb(stSockMsg,stUd):
	stNetData = g_hFd[stSockMsg.m_iFd]
	NetPack.FilterData(stUd,stSockMsg.m_sData)
	#stPack = NetPack.NetPack
	stPack = NetQueue.MsgPop(stUd)
	stPart = g_stWorkThread.m_stPart
	while stPack != None: 
		print("recv ip=%s port=%d data=%s"%(stNetData["ip"],stNetData["port"],stPack.m_sData))
		sData  = PackData(stPack.m_sData)
		#回显数据
		stPart.m_stNetServer.SendData(stSockMsg.m_iFd,sData)
		stPack = NetQueue.MsgPop(stUd)

#打包一个数据
def PackData(sData):
	sSendData  = struct.pack('!i',len(sData))
	sSendData += sData
	return sSendData	

#监听一个 sock
def ListenSockTest():
	sIp    = "0.0.0.0"
	iPort  = 8000
	stPart = g_stWorkThread.m_stPart
	stPart.m_stNetServer.DoListen(sIp,iPort,DataCb,AcceptCb,CloseCb)

#--------------------------------------------------
g_stWorkThread  = None
g_stNetThread   = None
g_stTimerThread = None 

class Watcher():  
	def __init__(self):  
		self.child = os.fork()  
		if self.child == 0:  
			return  
		else:  
			self.watch()  

	def watch(self):  
		try:  
			os.wait()  
		except KeyboardInterrupt:  
			self.kill()  
			sys.exit()  

	def kill(self):  
		try:  
			os.kill(self.child, signal.SIGKILL)  
		except OSError:  
			pass  

#主函数
if __name__ == '__main__':
	Watcher()
	g_stWorkThread  = WorkThread(1)
	g_stNetThread   = NetThread()
	g_stTimerThread = TimerThread()

	ConfigLog()

	#设置部件
	g_stWorkThread.m_stPart.m_stThread 		= g_stWorkThread
	#消息调度模块
	g_stWorkThread.m_stPart.m_stMsgQ    	= MsgQueue.MsgQueue(g_stWorkThread.m_stPart)
	#定时器模块
	g_stWorkThread.m_stPart.m_stTimerMgr    = g_stTimerThread.m_stTimerMgr
	#底层网络服务
	g_stWorkThread.m_stPart.m_stSockServer  = g_stNetThread.m_stSockServer
	#逻辑网络服务
	g_stWorkThread.m_stPart.m_stNetServer   = NetServer.NetServer(g_stWorkThread.m_stPart)

	ListenSockTest()

	#启动工作线程
	g_stWorkThread.start()
	#启动网络线程
	g_stNetThread.start()
	#启动定时器线程
	g_stTimerThread.start()
	print("------------server start wait client connect----------------")
	#工作线程等待
	g_stWorkThread.join()