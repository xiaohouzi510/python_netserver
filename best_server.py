#!/usr/bin/python
# encoding=utf-8

import time
import logging
import TimeMgr
import http_recv
import best_mysql
import thread,os,sys
import threading,time
import SocketServer
import MsgQueue
import NetServer 
import logging

logger = logging.getLogger(__name__)

g_szWorkThread = []
g_time_thread  = None
g_sock_thread  = None
g_index 	   = 0

#日志文件名
_DEBUG_FILE = './runlog.log'
_ERROR_FILE = './error_runlog.log'

def config_log():
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
		self.m_http_server   = None
		self.m_best_mysql    = None

#工作线程类
class WorkThread(threading.Thread):
    def __init__(self,iThreadID):
        threading.Thread.__init__(self)
        self.m_stPartsMgr = None
        self.m_iThreadID  = iThreadID
        self.m_sInfo      = None

    #线程循环
    def run(self):
        while True:
            self.OneLoop() and time.sleep(0.005)

    #一次循环
    def OneLoop(self):
        iMaxInterval  = 1000
        iCurTime      = GetMillisecond()
        bSleep        = self.m_stPartsMgr.m_stMsgQ.MsgLoop()
        iInterval     = GetMillisecond() - iCurTime
        bSleep        = not bSleep
        iInterval > iMaxInterval and logger.error("thread time out diffTime:%d"%(iInterval))
        return bSleep

    #获得 ID
    def GetThreadId(self):
        return self.m_iThreadID

#网络线程
class NetThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.m_stSockServer = SocketServer.SocketServer()

    #获得网络服务
    def GetSockServer(self):
        return self.m_stSockServer

    #线程循环
    def run(self):
        while True:
            self.m_stSockServer.ServerPoll()

http_work_thread = None
#定时器线程
class TimerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.m_stTimerMgr = TimeMgr.WheelTimerMgr()

    def run(self):
        while True:
            self.m_stTimerMgr.UpdateTime()
            time.sleep(0.0025)

#http 接收到数据
def http_recv_package(body,addr,fd,header):
	global g_index
	global http_work_thread
	thread_len = len(g_szWorkThread)
	g_index += 1
	if g_index >= thread_len:
		g_index = 0
	data = MsgQueue.ServerMessage()
	data.m_iId   = g_index + 1 
	data.m_sData = body
	MsgQueue.ForwardServer(data)
	http_work_thread.m_stPartsMgr.m_http_server.http_response(fd,"OK")

if __name__ == "__main__":
	config_log()
	#定时器
	g_time_thread = TimerThread()
	#api 网络
	g_sock_thread = NetThread()
	#工作服个数 
	work_thread_count = 20
	for i in xrange(work_thread_count):
		cur = WorkThread(i+1)
		cur.m_stPartsMgr = PartsMgr(cur)
		cur.m_stPartsMgr.m_stMsgQ       = MsgQueue.MsgQueue(cur.m_stPartsMgr)
		cur.m_stPartsMgr.m_stNetServer  = NetServer.NetServer(cur.m_stPartsMgr)	
		cur.m_stPartsMgr.m_best_mysql   = best_mysql.best_mysql(cur.m_stPartsMgr) 
		cur.m_stPartsMgr.m_best_mysql.init_consumer()
		g_szWorkThread.append(cur)
	#http 服
	http_work_thread = WorkThread(len(g_szWorkThread)+1)
	http_work_thread.m_stPartsMgr = PartsMgr(http_work_thread)
	http_work_thread.m_stPartsMgr.m_stMsgQ       = MsgQueue.MsgQueue(http_work_thread.m_stPartsMgr)
	http_work_thread.m_stPartsMgr.m_stNetServer  = NetServer.NetServer(http_work_thread.m_stPartsMgr)
	http_work_thread.m_stPartsMgr.m_http_server  = http_recv.http_recv(http_work_thread.m_stPartsMgr,http_recv_package,False)
	http_work_thread.m_stPartsMgr.m_stTimerMgr   = g_time_thread.m_stTimerMgr
	http_work_thread.m_stPartsMgr.m_stSockServer = g_sock_thread.m_stSockServer
	#启动 http 8888 端口
	http_work_thread.m_stPartsMgr.m_http_server.start_server("0.0.0.0",8888)
	g_time_thread.start()
	g_sock_thread.start()
	http_work_thread.start()
	for k,v in enumerate(g_szWorkThread):
		v.start()
	for k,v in enumerate(g_szWorkThread):
		v.join()