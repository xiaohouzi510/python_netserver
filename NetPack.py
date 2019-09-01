#!/usr/bin/python
# encoding=utf-8

import NetQueue
import struct

#包数据
class NetPack:
	def __init__(self,sData = None):	
		self.m_sData  = sData
		self.m_iLen   = 0
		self.m_stNext = None

#未完成数据
class UnComplete:
	def __init__(self):
		self.m_iRead = -2
		self.m_iLen  = 0
		self.m_sData = None

#包队列
class PackQueue:
	def __init__(self):
		self.m_stHead   	= None
		self.m_stTail   	= None
		self.m_iCount   	= 0
		self.m_stLock 		= None
		#未完整的一个包
		self.m_stUn 	    = UnComplete()

#头部大小
def HeadSize(sData):
	return struct.unpack('>i',sData[:4])[0]

#链表中加入一个包
def PushData(stQueue,sData):
	NetQueue.AddTail(stQueue,NetPack(sData))

#处理接收到数据
def PushMore(stQueue,sData,iLen):
	if iLen < 4:
		stQueue.m_stUn.m_sData = sData
		stQueue.m_stUn.m_iRead = -1
		return
	iPackSize = HeadSize(sData)
	iLen = iLen - 4
	if iPackSize > iLen:
		stQueue.m_stUn.m_iRead = iLen
		stQueue.m_stUn.m_iLen  = iPackSize
		stQueue.m_stUn.m_sData = sData[4:]
		return
	#链表中加入一个包
	PushData(stQueue,sData[4:(4+iPackSize)])
	iLen = iLen - iPackSize
	if iLen > 0:
		PushMore(stQueue,sData[4+iPackSize:],iLen) 

#重置未完成数据
def ResetUn(stQueue):
	stQueue.m_stUn.m_sData  = None 
	stQueue.m_stUn.m_iLen   = 0
	stQueue.m_stUn.m_iRead 	= -2

#粘包
def FilterData(stQueue,sData):
	iLen = len(sData)
	#没有未完成的包
	if stQueue.m_stUn.m_iRead == -2:
		PushMore(stQueue,sData,iLen)
		return
	#sData 表示已被使用过多少长度
	iUse = 0
	if stQueue.m_stUn.m_iRead == -1:
		iReadLen = len(stQueue.m_stUn.m_sData)
		#小于 4 字节的数据，表示还未达到包长度
		if iReadLen + iLen < 4:
			stQueue.m_stUn.m_sData = stQueue.m_stUn.m_sData + sData
			return
		iUse = 4 - iReadLen
		iLen = iLen - iUse
		iPackSize = HeadSize(stQueue.m_stUn.m_sData + sData[:iUse])
		stQueue.m_stUn.m_iLen  = iPackSize
		stQueue.m_stUn.m_sData = ""
		stQueue.m_stUn.m_iRead = 0
	iNeed = stQueue.m_stUn.m_iLen - stQueue.m_stUn.m_iRead	
	if iNeed > iLen:
		stQueue.m_stUn.m_iRead = stQueue.m_stUn.m_iRead + iLen	
		stQueue.m_stUn.m_sData = stQueue.m_stUn.m_sData + sData[iUse:]
		return
	iLen = iLen - iNeed
	sPackData = stQueue.m_stUn.m_sData + sData[iUse:(iUse + iNeed)]
	iUse = iUse + iNeed
	ResetUn(stQueue)
	#链表中加入一个包
	PushData(stQueue,sPackData)
	if iLen <= 0:
		return
	PushMore(stQueue,sData[iUse:],iLen)