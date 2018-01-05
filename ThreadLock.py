#!/usr/bin/python
# coding=utf-8

import thread

#----------锁--------------
class ThreadLock:
	#初始化
	def __init__(self):
		#暂时用 python thread lock
		self.m_stLock = thread.allocate_lock()

	#上锁
	def Lock(self):
		self.m_stLock.acquire()

	#解锁
	def UnLock(self):
		self.m_stLock.release()