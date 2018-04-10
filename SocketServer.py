#!/usr/bin/python
# encoding=utf-8

import multiprocessing
import ThreadLock 
import select
import threading
import socket
import NetQueue
import logging
import errno
import NetServer
import MsgQueue
import TimeMgr
import os

logger = logging.getLogger(__name__)

MIN_READ_BUFFER = 64
MAX_UDP_PACKAGE = 65535

#命令类型
class CmdType:
	eCmdListenType = 'L' 		#监听
	eCmdOpenType   = 'O' 		#打开一个 tcp 连接
	eCmdDataType   = 'D' 		#发送数据
	eCmdUdpType    = 'U' 		#创建一个 udp 连接
	eCmdUConnect   = 'C' 		#udp 设置发送地址
	eCmdCloseType  = 'K'  		#关闭一个 socket

#socket 类型
class SockType:
	eSockInvalid    = 0
	eSockListen     = 1 		#监听
	eSockConnecting = 3 		#三次握手中
	eSockConnected  = 4 		#已成功建立连接
	eSockClosing    = 5 		#正在关闭中

#执行 cmd 返回码
class CmdErrno:
	eCmdInvalid  = -2
	eCmdError    = -1
	eCmdSuccess  = 0
	eCmdListen   = 1
	eCmdConnect  = 2
	eCmdAccept   = 3
	eCmdClose    = 4
	eCmdData 	 = 5
	eCmdUdp 	 = 6

#写缓存
class WriteBuffer:
	def __init__(self):
		self.m_stNext    = None
		self.m_stData    = None
		self.m_iOffset   = 0
		self.m_stUdpAddr = None
		self.m_iSize 	 = 0

#写列表
class WriteList:
	def __init__(self):
		#指向 WriteBuffer
		self.m_stHead = None	
		self.m_stTail = None
		self.m_stLock = None
		self.m_iCount = None

#--------一个 fd 数据-----------
class FdData:
	def __init__(self):
		self.m_stWbList  = WriteList()
		self.m_eType     = SockType.eSockInvalid
		self.m_iProtocol = 0
		self.m_stSock    = None
		self.m_iId       = 0
		self.m_iFd 		 = 0
		self.m_iSize 	 = MIN_READ_BUFFER
		self.m_stUdpAddr = None

#sock 消息
class SockMessage:
	def __init__(self):
		# m_eType = CmdErrno
		self.m_eType  = 0   		 
		self.m_iFd    = 0 
		self.m_iId    = 0 
		self.m_sData  = None

#命令链表
class CmdLink:
	def __init__(self):
		# CmdBase
		self.m_stHead = None
		self.m_stTail = None 
		self.m_stLock = ThreadLock.ThreadLock()
		self.m_iCount = 0

#命令基类
class CmdBase:
	def __init__(self,cFlag,iFd,iId):
		self.m_cFlag  = cFlag 
		self.m_iFd    = iFd
		self.m_iId    = iId
		self.m_stNext = None

#发送数据命令
class SendDataCmd(CmdBase):
	def __init__(self,cFlag,iFd,iId,sData,stUdpAddr=None): 
		CmdBase.__init__(self,cFlag,iFd,iId)
		self.m_sData  	 = sData
		self.m_stUdpAddr = stUdpAddr 

#监听命令
class ListenCmd(CmdBase):
	def __init__(self,cFlag,iFd,iId,stSock):
		CmdBase.__init__(self,cFlag,iFd,iId)
		self.m_stSock  = stSock 

#连接命命
class ConnectCmd(CmdBase):
	def __init__(self,cFlag,iFd,iId,stSock,sIp,iPort):
		CmdBase.__init__(self,cFlag,iFd,iId)
		self.m_stSock  = stSock 
		self.m_sIp     = sIp
		self.m_iPort   = iPort

#udp 
class UdpCmd(CmdBase):
	def __init__(self,cFlag,iFd,iId,stSock):
		CmdBase.__init__(self,cFlag,iFd,iId)
		self.m_stSock  = stSock

#udp 设置发送地址
class UdpSendAddrCmd(CmdBase):
	def __init__(self,cFlag,iFd,iId,stAddr):
		CmdBase.__init__(self,cFlag,iFd,iId)
		self.m_stUdpAddr = stAddr

#关闭 socket
class CloseSockCmd(CmdBase):
	def __init__(self,cFlag,iFd,iId):
		CmdBase.__init__(self,cFlag,iFd,iId)

#--------------socket 服务----------------------
class SocketServer:
	def __init__(self):	
		#epoll 对象
		self.m_stEpoll    = None 
		#读管道
		self.m_stRdPipe   = None
		#写管道
		self.m_stWrPipe   = None
		#所有 fd 数据
		self.m_hFdData    = {}
		#fileno 对应 fd data
		self.m_hFileD     = {}
		#命令链表
		self.m_stCmd      = CmdLink()
		#临时事件
		self.m_szTmpEvent = None
		#命令、sock 交替处理
		self.m_iCheckCtrl = 1
		#事件个数
		self.m_iEventCount= 0
		#事件索引
		self.m_iEventIndex= 0
		#fd lock
		self.m_stFdLock   = ThreadLock.ThreadLock()
		#生成 fd，系统 fd 用 iFileNo 表示
		self.m_iFdAlloc   = 0
		#epoll wait time
		self.m_iWaitTime  = -1 
		#初始化
		self.InitServer()

	#设置 eopll 等时间，单线程情况会直接返回，多线程要设置为 -1 
	def SetWaitTime(self,iTime):
		self.m_iWaitTime = iTime

	#生成 fd
	def MakeFd(self):
		self.m_stFdLock.Lock()
		self.m_iFdAlloc = (self.m_iFdAlloc + 1)&TimeMgr.TICK_MASK
		while self.m_iFdAlloc == 0:
			self.m_iFdAlloc = (self.m_iFdAlloc + 1)&TimeMgr.TICK_MASK
		self.m_stFdLock.UnLock()
		return self.m_iFdAlloc

	#加入 epoll
	def EpollAdd(self,iFd):
		try: 
			self.m_stEpoll.register(iFd,select.EPOLLIN)	
		except socket.error,arg:
			eno, err_msg = arg
			logger.error("epoll add error eno=%d fd=%d msg=%s"%(eno,iFd,err_msg))
			return False
		return True

	#epoll write 事件
	def EpollWrite(self,iFileNo,bWrite): 
		try: 
			iEvent = select.EPOLLIN | (bWrite and select.EPOLLOUT or 0)
			self.m_stEpoll.modify(iFileNo,iEvent)
		except socket.error,arg:
			eno, err_msg = arg
			logger.error("epoll add error eno=%d fd=%d msg=%s"%(eno,iFd,err_msg))
			return False
		return True

	#删除 epoll 中的 fd
	def EpollDel(self,iFd):
		try:  
			self.m_stEpoll.unregister(iFd)
		except socket.error,arg:
			logger.error("epoll del error eno=%d fd=%d msg=%s"%(eno,iFd,err_msg))
			return False
		return True

	#清理所有数据
	def Release(self):	
		#关闭读管道
		if self.m_stRdPipe != None:
			self.m_stRdPipe.close()
			self.m_stRdPipe = None
		#关闭写管道
		if self.m_stWrPipe != None:
			self.m_stWrPipe.close()
			self.m_stWrPipe = None
		#关闭 epoll
		if self.m_stEpoll != None:
			self.m_stEpoll.close()
			self.m_stEpoll = None

	#初始化网络服务 
	def InitServer(self):
		#创建 epoll
		self.m_stEpoll = select.epoll()
		#创建管道
		self.m_stWrPipe,self.m_stRdPipe = multiprocessing.Pipe()
		#加入 epoll
		if not self.EpollAdd(self.m_stWrPipe.fileno()):
			self.Release()
			return False
		return True

	#push cmd
	def PushCmd(self,stCmd):
		NetQueue.AddTail(self.m_stCmd,stCmd)
		#唤醒 epoll
		self.m_iWaitTime != 0 and self.m_stRdPipe.send(['C'])

	#sock 循环
	def ServerPoll(self):
		stSockMsg = SockMessage()
		stSockMsg.m_eType = self.SockPoll(stSockMsg)	
		if stSockMsg.m_eType == CmdErrno.eCmdInvalid:
			return False
		elif stSockMsg.m_eType != CmdErrno.eCmdSuccess:
			MsgQueue.ForwardSockMsg(stSockMsg)
		return True

	#server 循环
	def SockPoll(self,stResult):
		while True: 
			#优先处理命令，即处理逻辑线程事务
			if self.m_iCheckCtrl != 0:
				stCmd = NetQueue.MsgPop(self.m_stCmd)
				if stCmd != None:
					stResult.m_iId = stCmd.m_iId
					stResult.m_iFd = stCmd.m_iFd  
					eType = self.CtrlCmd(stCmd,stResult)
					if eType == CmdErrno.eCmdSuccess:
						continue
					else:
						return eType
				else:
					self.m_iCheckCtrl = 0
			#没有未完成事件
			if self.m_iEventCount == self.m_iEventIndex:
				self.m_szTmpEvent  = self.m_stEpoll.poll(self.m_iWaitTime) 
				self.m_iCheckCtrl  = 1
				self.m_iEventIndex = 0
				self.m_iEventCount = len(self.m_szTmpEvent)
				if self.m_iEventCount == 0:
					return CmdErrno.eCmdInvalid 
			iFileNo,iEvent = self.m_szTmpEvent[self.m_iEventIndex]
			self.m_iEventIndex = self.m_iEventIndex + 1
			#管道数据只负责唤醒 epoll，管道未用做队列，因为没有指针
			if self.m_stWrPipe.fileno() == iFileNo:
				sData = self.m_stWrPipe.recv()
				continue	
			stFd = self.GetFdDataByFileNo(iFileNo)
			if stFd == None:
				logger.warning("epoll event not found data fileno=%d"%(iFileNo))
				continue 
			stResult.m_iId = stFd.m_iId
			#listen fd
			if stFd.m_eType == SockType.eSockListen:	
				eCode = self.ReportAccept(stFd,stResult)
				if eCode == CmdErrno.eCmdError:
					return CmdErrno.eCmdError
				elif eCode == CmdErrno.eCmdSuccess:
					continue
				return eCode
			#connect fd
			elif stFd.m_eType == SockType.eSockConnecting:
				return self.ReportConnect(stFd,stResult)
			else:
				bRead  = iEvent & select.EPOLLIN
				bWrite = iEvent & select.EPOLLOUT
				bError = iEvent & select.EPOLLERR
				if bRead:
					eCode = 0	
					if stFd.m_iProtocol == socket.IPPROTO_TCP:
						eCode = self.RecvTcp(stFd,stResult)
					else:
						eCode = self.RecvUdp(stFd,stResult)
					#udp 读到数据，下次再读一次
					if eCode == CmdErrno.eCmdUdp:
						self.m_iEventIndex = self.m_iEventIndex - 1
						return CmdErrno.eCmdUdp 
					#如果有写事件，并且连接没有错误，下次处理写事件
					if bWrite and eCode != CmdErrno.eCmdClose and eCode != CmdErrno.eCmdError:
						self.m_iEventIndex = self.m_iEventIndex - 1
						#清除 in 事件
						self.m_szTmpEvent[self.m_iEventIndex] = (iFileNo,(iEvent & ~select.EPOLLIN))
					if eCode == CmdErrno.eCmdSuccess:
						break	
					return eCode 
				if bWrite:
					eCode = 0
					if stFd.m_iProtocol == socket.IPPROTO_TCP:
						eCode = self.SendTcp(stFd,stResult)
					else:
						eCode = self.SendUdp(stFd,stResult)
					if eCode == CmdErrno.eCmdSuccess:
						break	
					return eCode  
				if bError:
					iError = stFd.m_stSock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
					self.ForceClose(stFd.m_iFd,iError,os.strerror(iError))
					stResult.m_iFd = stFd.m_iFd
					return CmdErrno.eCmdError
		return CmdErrno.eCmdSuccess

	#发送 tcp 数据
	def SendTcp(self,stFd,stResult):
		#类型不为 SockType.eSockConnected，不可以发送数据
		if stFd.m_eType != SockType.eSockConnected and stFd.m_eType != SockType.eSockClosing:
			return  CmdErrno.eCmdSuccess
		stResult.m_iFd = stFd.m_iFd
		while stFd.m_stWbList.m_stHead != None:
			stNode = stFd.m_stWbList.m_stHead
			try: 
				iSendSize = stFd.m_stSock.send(stNode.m_stData[stNode.m_iOffset:])
			except socket.error, arg:
				eno, err_msg = arg
				if eno == errno.EAGAIN:
					return CmdErrno.eCmdSuccess
				elif eno == errno.EINTR:
					continue
				else:
					self.ForceClose(stFd.m_iFd,eno,err_msg)	
					return CmdErrno.eCmdClose
			stNode.m_iOffset = stNode.m_iOffset + iSendSize
			#没有发送完成，等待下次再发
			if stNode.m_iOffset < stNode.m_iSize: 
				return CmdErrno.eCmdSuccess
			stFd.m_stWbList.m_stHead = stNode.m_stNext
		stFd.m_stWbList.m_stTail = None
		self.EpollWrite(stFd.m_stSock.fileno(),False)
		if stFd.m_eType == SockType.eSockClosing:
			self.ForceClose(stFd.m_iFd,0,"manual closure socket")
		return CmdErrno.eCmdSuccess

	#发送 udp 数据
	def SendUdp(self,stFd,stResult):
		stResult.m_iFd = stFd.m_iFd
		while stFd.m_stWbList.m_stHead != None:
			stNode = stFd.m_stWbList.m_stHead
			try: 
				stUdpAddr = stNode.m_stUdpAddr == None and stFd.m_stUdpAddr or stNode.m_stUdpAddr
				stFd.m_stSock.sendto(stNode.m_stData,stUdpAddr)
			except socket.error, arg:
				eno, err_msg = arg
				#忽略 udp 错误
				if eno == errno.EAGAIN or eno == errno.EINTR:
					return CmdErrno.eCmdSuccess
				else:
					return mdErrno.eCmdSuccess
			stFd.m_stWbList.m_stHead = stNode.m_stNext
		self.EpollWrite(stFd.m_stSock.fileno(),False)
		stFd.m_stWbList.m_stTail = None
		return CmdErrno.eCmdSuccess

	#接收 tcp 数据
	def RecvTcp(self,stFd,stResult):
		try: 
			stResult.m_iFd   = stFd.m_iFd
			stResult.m_sData = stFd.m_stSock.recv(stFd.m_iSize)	
			#空字符串表示对方断开连接
			if not stResult.m_sData:
				self.ForceClose(stFd.m_iFd,0,"close by peer 10086")	
				return CmdErrno.eCmdClose
		except socket.error, arg:
			eno, err_msg = arg
			if eno != errno.EAGAIN and eno != errno.EINTR:
				self.ForceClose(stFd.m_iFd,eno,err_msg)	
				return CmdErrno.eCmdClose
			return CmdErrno.eCmdSuccess
		iSize = len(stResult.m_sData)
		if iSize == stFd.m_iSize:
			stFd.m_iSize = stFd.m_iSize*2 
		elif stFd.m_iSize > MIN_READ_BUFFER and iSize*2 < stFd.m_iSize:
			stFd.m_iSize = stFd.m_iSize/2
		return CmdErrno.eCmdData

	#接收 udp
	def RecvUdp(self,stFd,stResult):
		try:  
			stResult.m_iFd = stFd.m_iFd
			stResult.m_sData,stResult.m_stUdpAddr = stFd.m_stSock.recvfrom(MAX_UDP_PACKAGE)
		except socket.error, arg:
			eno, err_msg = arg
			if eno != errno.EAGAIN and eno != errno.EINTR:
				self.ForceClose(stFd.m_iFd,eno,err_msg)	
				return CmdErrno.eCmdClose
			return CmdErrno.eCmdSuccess
		return CmdErrno.eCmdUdp

	#处理接受
	def ReportAccept(self,stFd,stResult):
		stResult.m_iId = stFd.m_iId
		try: 
			stNewSock,stAddr = stFd.m_stSock.accept()
			self.SockKeepAlive(stNewSock)
			stNewSock.setblocking(0)
			if not self.EpollAdd(stNewSock.fileno()):
				logger.error("accept add epoll error id=%d fd=%d",stFd.m_iId,stFd.m_iFd)
				stNewSock.close()
				return CmdErrno.eCmdSuccess
		except socket.error, arg:
			eno, err_msg = arg 
			logger.error("accept error id=%d fd=%d",stFd.m_iId,stFd.m_iFd)
			return CmdErrno.eCmdSuccess
		stNewFd = self.CreateSockData(self.MakeFd(),stFd.m_iId,socket.IPPROTO_TCP,stNewSock) 
		stNewFd.m_eType  = SockType.eSockConnected
		stResult.m_sData = [stAddr,stFd.m_iFd]
		stResult.m_iFd   = stNewFd.m_iFd
		return CmdErrno.eCmdAccept

	#连接
	def ReportConnect(self,stFd,stResult):
		try: 
			stResult.m_iFd = stFd.m_iFd
			eCode = stFd.m_stSock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
			if eCode != 0:
				self.ForceClose(stFd.m_iFd,eCode,os.strerror(eCode))
				return CmdErrno.eCmdClose
		except socket.error, arg:
			eno, err_msg = arg 
			logger.error("three head error id=%d fd=%d",stFd.m_iId,stFd.m_iFd)
			self.ForceClose(stFd.m_iFd,eno,err_msg)
			return CmdErrno.eCmdClose
		stFd.m_eType   = SockType.eSockConnected
		#三次握手成功，没有数据则取消 out 事件
		if self.BufferEmpty(stFd.m_stWbList):
			self.EpollWrite(stFd.m_stSock.fileno(),False)
		return CmdErrno.eCmdConnect

	#设置 keepalive
	def SockKeepAlive(self,stSock):
		stSock.setsockopt(socket.SOL_SOCKET,socket.SO_KEEPALIVE,1)

	#获得一个 fd data
	def GetFdData(self,iFd):
		if not self.m_hFdData.has_key(iFd):
			return None
		return self.m_hFdData[iFd]

	#获得 fd data
	def GetFdDataByFileNo(self,iFileNo):
		if not self.m_hFileD.has_key(iFileNo):
			return None
		return self.m_hFileD[iFileNo]

	#关闭一个 socket
	def ForceClose(self,iFd,iErrno,sError):
		stFd = self.GetFdData(iFd)
		if stFd == None:
			logger.error("force close not found fd=%d"%(iFd))
			return
		del self.m_hFdData[iFd]
		del self.m_hFileD[stFd.m_stSock.fileno()]
		self.EpollDel(stFd.m_stSock.fileno())
		logger.debug("force close fd=%d fileno=%d errno=%d strerror=%s"%(iFd,stFd.m_stSock.fileno(),iErrno,sError))
		stFd.m_stSock.close()

	#创建一个 sockData
	def CreateSockData(self,iFd,iId,iProtocol,stSock):
		stFd = self.GetFdData(iFd)
		if stFd != None:
			logger.error("double socket data fd=%d id=%d"%(iFd,iId))
			return stFd
		stFd = FdData()
		stFd.m_iFd = iFd
		stFd.m_iId = iId
		stFd.m_iProtocol = iProtocol
		stFd.m_stSock    = stSock
		self.m_hFdData[iFd] = stFd
		self.m_hFileD[stSock.fileno()] = stFd
		return stFd

	#处理命令事件
	def CtrlCmd(self,stCmd,stResult):
		#监听命令
		if stCmd.m_cFlag == CmdType.eCmdListenType:
			return self.DealListen(stCmd,stResult)
		#打开一个连接
		elif stCmd.m_cFlag == CmdType.eCmdOpenType:
			return self.DealConnect(stCmd,stResult)
		#发送数据
		elif stCmd.m_cFlag == CmdType.eCmdDataType:
			return self.DealSendData(stCmd,stResult)
		#udp
		elif stCmd.m_cFlag == CmdType.eCmdUdpType:
			return self.DealUdp(stCmd,stResult)
		#udp 绑定发送地址
		elif stCmd.m_cFlag == CmdType.eCmdUConnect:
			return self.DealUdpConnect(stCmd,stResult)	
		#关闭 socket
		elif stCmd.m_cFlag == CmdType.eCmdCloseType:
			return self.DealCloseSock(stCmd,stResult)
		else:
			logger.error("error cmd flag=%s fd=%d id=%d"%(stCmd.m_cFlag,stCmd.m_iFd,stCmd.m_iId))

	#打开一个连接
 	def DealConnect(self,stCmd,stResult):
		try: 
			iEno = stCmd.m_stSock.connect((stCmd.m_sIp,int(stCmd.m_iPort)))
		except socket.error, arg:
			iEno,err_msg = arg
			if iEno != errno.EINPROGRESS:
				stCmd.stSock.close()
				logger.error("deal connect error id=%d ip=%s port=%d iEno=%d err=%s"%(stCmd.m_iId,stCmd.m_sIp,stCmd.m_iPort,iEno,err_msg))
				return CmdErrno.eCmdClose
		stFd = self.CreateSockData(stCmd.m_iFd,stCmd.m_iId,socket.IPPROTO_TCP,stCmd.m_stSock)
		self.EpollAdd(stFd.m_stSock.fileno())
		if iEno != 0:
			self.EpollWrite(stFd.m_stSock.fileno(),True)
			stFd.m_eType = SockType.eSockConnecting
			return CmdErrno.eCmdSuccess
		else:
			stFd.m_eType = SockType.eSockConnected
			return CmdErrno.eCmdConnect

	#处理监听
	def DealListen(self,stCmd,stResult):
		if not self.EpollAdd(stCmd.m_stSock.fileno()):
			logger.error("deal listen add error fd=%d id=%d"%(stCmd.m_iFd,stCmd.m_iId))
			print("deal listen add error fd=%d id=%d"%(stCmd.m_iFd,stCmd.m_iId))
			stCmd.m_stSock.close()
			return CmdErrno.eCmdClose
		stFd = self.CreateSockData(stCmd.m_iFd,stCmd.m_iId,socket.IPPROTO_TCP,stCmd.m_stSock) 
		stFd.m_eType  = SockType.eSockListen
		# print("listen success fd=%d id=%d"%(stCmd.m_iFd,stCmd.m_iId))
		return CmdErrno.eCmdListen

	#判断缓存是否为空
	def BufferEmpty(self,stLink):
		return stLink.m_stHead == None 

	#发送数据
	def DealSendData(self,stCmd,stResult):
		stFd = self.GetFdData(stCmd.m_iFd)
		if stFd == None:
			logger.error("send data not found fd=%d id=%d"%(stCmd.m_iFd,stCmd.m_iId))
			return CmdErrno.eCmdSuccess
		#connect 不成功不发送数据，握手成功后再发送
		if stFd.m_eType == SockType.eSockConnected and self.BufferEmpty(stFd.m_stWbList):
			self.EpollWrite(stFd.m_stSock.fileno(),True)	
		stBuffer = WriteBuffer()
		stBuffer.m_stData    = stCmd.m_sData
		stBuffer.m_iSize     = len(stCmd.m_sData)
		stBuffer.m_stUdpAddr = stCmd.m_stUdpAddr 
		NetQueue.AddTail(stFd.m_stWbList,stBuffer)
		return CmdErrno.eCmdSuccess

	#udp
	def DealUdp(self,stCmd,stResult):
		if not self.EpollAdd(stCmd.m_stSock.fileno()):
			stCmd.m_stSock.close()
			return CmdErrno.eCmdClose
		stFd = self.CreateSockData(stCmd.m_iFd,stCmd.m_iId,socket.IPPROTO_UDP,stCmd.m_stSock) 
		stFd.m_eType = SockType.eSockConnected
		return CmdErrno.eCmdSuccess

	#udp 绑定发送地址
	def DealUdpConnect(self,stCmd,stResult):
		stFd = self.GetFdData(stCmd.m_iFd)
		if stFd == None:
			logger.error("udp connect not found data fd=%d id=%d"%(stCmd.m_iFd,stCmd.m_iId))
			return CmdErrno.eCmdError
		stFd.m_stUdpAddr = stCmd.m_stUdpAddr
		return CmdErrno.eCmdSuccess

	#关闭 socket
	def DealCloseSock(self,stCmd,stResult):
		stFd = self.GetFdData(stCmd.m_iFd)
		if stFd == None:
			logger.error("colse sock not found data fd=%d id=%d"%(stCmd.m_iFd,stCmd.m_iId))
			return CmdErrno.eCmdSuccess
		if self.BufferEmpty(stFd.m_stWbList): 
			self.ForceClose(stCmd.m_iFd,0,"manual closure socket")
		else:
			stFd.m_eType = SockType.eSockClosing 
		return CmdErrno.eCmdSuccess