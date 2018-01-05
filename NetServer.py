#!/usr/bin/python
# encoding=utf-8

import NetQueue
import MsgQueue
import NetPack
import socket
import logging
import SocketServer

logger = logging.getLogger(__name__)

#fd 数据
class NetFdData:
    def __init__(self,iFd,fDataCb,fCSuccessCb,fCloseCb):
        self.m_iFd          = iFd
        #接收到数据回调
        self.m_fDataCb      = fDataCb 
        #连接成回调       
        self.m_fCSuccessCb  = fCSuccessCb
        #断开连接回调
       	self.m_fCloseCb     = fCloseCb 
        #用户数据 
        self.m_stUd 	    = None 

#------------网络服务---------------
class NetServer:
	def __init__(self,stPartsMgr):
		#部件
		self.m_stPartsMgr = stPartsMgr
		#NetFdData
		self.m_hFdData    = {}
		#网络回调
		stPartsMgr.m_stMsgQ.RegMsg(MsgQueue.MsgData(MsgQueue.MsgQType.eMsgNetSock,self.NetDispatchMsg))

	#listen 成功
	def ListenCb(self,stSockMsg):
		return	

	#connect 成功
	def ConnectCb(self,stSockMsg):
		logger.debug("connect cb fd=%d"%(stSockMsg.m_iFd))
		stFd = self.GetFdData(stSockMsg.m_iFd)
		if stFd == None:
			print("connect cb not found data fd=%d"%(stSockMsg.m_iFd))
			logger.error("connect cb not found data fd=%d"%(stSockMsg.m_iFd))
			return
		if stFd.m_fCSuccessCb != None: 
			stFd.m_fCSuccessCb(stFd) 
		else:
			stFd.m_stUd = NetPack.PackQueue() 
		logger.debug("connect success fd=%d"%(stSockMsg.m_iFd))	

	#accept 成功
	def AcceptCb(self,stSockMsg):
		stLFd = self.GetFdData(stSockMsg.m_sData[1])
		stAFd = self.CreateFdData(stLFd.m_fDataCb,stLFd.m_fCSuccessCb,stLFd.m_fCloseCb,stSockMsg.m_iFd)
		stAFd.m_fCSuccessCb != None and stAFd.m_fCSuccessCb(stAFd,stSockMsg.m_sData[0])	

	#close fd
	def CloseCb(self,stSockMsg):
		stFd = self.GetFdData(stSockMsg.m_iFd)
		if stFd == None:
			logger.error("close not found fd=%d data=%s"%(stSockMsg.m_iFd,stSockMsg))
			return False
		stFd.m_fCloseCb != None and stFd.m_fCloseCb(stFd)
		logger.debug("close fd=%d data=%s"%(stSockMsg.m_iFd,stSockMsg))
		del self.m_hFdData[stSockMsg.m_iFd]
		return True

	#接收到数据
	def NetDataCb(self,stSockMsg):
		stFd = self.GetFdData(stSockMsg.m_iFd)
		if stFd == None:
			logger.error("fd recv data not found fd=%d data=%s"%(stSockMsg.m_iFd,stSockMsg))
			return
		stFd.m_fDataCb(stSockMsg,stFd.m_stUd)

	#创建 fd data
	def CreateFdData(self,fDataCb,fCSuccessCb,fCloseCb,iFd=None):
		iFd = iFd != None and iFd or self.m_stPartsMgr.m_stSockServer.MakeFd() 
		if self.m_hFdData.has_key(iFd):
			stData = self.m_hFdData[iFd]
			logger.error("double net server fd=%d data=%s"%(iFd,stData))
			return stData
		stData = NetFdData(iFd,fDataCb,fCSuccessCb,fCloseCb)
		self.m_hFdData[iFd] = stData
		logger.debug("net server create fd=%d data=%s"%(iFd,stData))
		return stData

	#获得 fd data
	def GetFdData(self,iFd):
		if not self.m_hFdData.has_key(iFd):
			return None
		return self.m_hFdData[iFd]

	#获得线程 Id
	def GetThreadId(self):
		return self.m_stPartsMgr.m_stThread.GetThreadId()

	#网络消息转发
	def NetDispatchMsg(self,stSockMsg):
		if stSockMsg.m_eType == SocketServer.CmdErrno.eCmdError:
			self.CloseCb(stSockMsg)
		elif stSockMsg.m_eType == SocketServer.CmdErrno.eCmdListen: 
			self.ListenCb(stSockMsg)
		elif stSockMsg.m_eType == SocketServer.CmdErrno.eCmdConnect:
			self.ConnectCb(stSockMsg)
		elif stSockMsg.m_eType == SocketServer.CmdErrno.eCmdAccept:
			self.AcceptCb(stSockMsg)
		elif stSockMsg.m_eType == SocketServer.CmdErrno.eCmdClose:
			self.CloseCb(stSockMsg)
		elif stSockMsg.m_eType == SocketServer.CmdErrno.eCmdData:
			self.NetDataCb(stSockMsg)
		elif stSockMsg.m_eType == SocketServer.CmdErrno.eCmdUdp:
			self.NetDataCb(stSockMsg)
		else:
			logger.error("net dispatch error type=%d fd=%d data=%s",stSockMsg.m_eType,stSockMsg.m_iFd,stSockMsg.m_sData)

	#监听一个端口
	def DoListen(self,sIp,iPort,fDCb,fCCb,fCloseCb):
		stSock = self.DoBind(sIp,iPort,socket.IPPROTO_TCP)
		if stSock == None:
			return -1 
		try:
			stSock.listen(32)
		except socket.error,arg:
			stSock.close()
			eno,err_msg = arg
			logger.error("listen error ip=%s port=%d eno=%d err_msg=%s"%(sIp,iPort,eno,err_msg))
			return -1 
		stFd  = self.CreateFdData(fDCb,fCCb,fCloseCb)	
		iFd   = stFd.m_iFd
		stCmd = SocketServer.ListenCmd(SocketServer.CmdType.eCmdListenType,iFd,self.GetThreadId(),stSock)
		self.m_stPartsMgr.m_stSockServer.PushCmd(stCmd)
		return iFd

	#连接
	#param sIp ip 地址
	#param iPort 端口
	#param fDCb 收到数据回调，listen 类型的 accept 时的 fd 接收到数据回调
	#param fCCb 连接成功时回调
	def DoConnect(self,sIp,iPort,fDCb,fCCb,fCloseCb):
		try:
			stSock = self.CreateSock(socket.IPPROTO_TCP)
		except socket.error, arg:
			eno, err_msg = arg
			logger.error("connect sock error id=%d ip=%s port=%d eno=%d err_msg=%s"%(stCmd.m_Id,sIp,iPort,eno,err_msg))
			return -1 
		stFd  = self.CreateFdData(fDCb,fCCb,fCloseCb)	
		iFd   = stFd.m_iFd
		#打开一个连接
		sFlag = SocketServer.CmdType.eCmdOpenType
		stCmd = SocketServer.ConnectCmd(sFlag,iFd,self.GetThreadId(),stSock,sIp,iPort)
		self.m_stPartsMgr.m_stSockServer.PushCmd(stCmd)
		logger.debug("do connect ip=%s port=%d fd=%d fileno=%d"%(sIp,iPort,iFd,stSock.fileno()))
		return iFd

	#创建 sock
	def CreateSock(self,iProtocol):
		iSockType = None 
		if iProtocol == socket.IPPROTO_TCP: 
			iSockType = socket.SOCK_STREAM
		else:
			iSockType = socket.SOCK_DGRAM
		try:
			logger.debug("proto type=%d tcp=%d"%(iSockType,socket.SOCK_STREAM))
			stSock = socket.socket(socket.AF_INET,iSockType)
			stSock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
			stSock.setblocking(0)
		except socket.error, arg:
			eno, err_msg = arg
			logger.error("create sock error eno=%d err_msg=%s"%(eno,err_msg))
			return None
		return stSock

	#绑定
	def DoBind(self,sIp,iPort,iProtocol):
		stSock = self.CreateSock(iProtocol) 
		if stSock == None:
			return None
		try: 
			stSock.bind((sIp,iPort))
		except socket.error, arg:
			stSock.close()
			eno,err_msg = arg
			logger.error("bind error ip=%s port=%d eno=%d err_msg=%s"%(sIp,iPort,eno,err_msg))
			return None
		return stSock

	#创建 udp
	#param fDCb   收到数据回调
	#param fCCb   放入 epoll 成功后回调
	#param sIp    绑定的 ip，为 None 则不绑定
	#param iPort  梆定的端口，不为 None 时 sIp 也不能为 None
	def DoUdp(self,fDCb = None,sIp = None,iPort = None):
		stSock = None
		if sIp != None and iPort != None:
			stSock = self.DoBind(sIp,iPort,socket.IPPROTO_UDP)
		else:
			stSock = self.CreateSock(socket.IPPROTO_UDP) 
		stFd  = self.CreateFdData(fDCb,None,None)	
		iFd   = stFd.m_iFd
		stCmd = SocketServer.UdpCmd(SocketServer.CmdType.eCmdUdpType,iFd,self.GetThreadId(),stSock)
		self.m_stPartsMgr.m_stSockServer.PushCmd(stCmd)
		return iFd

	#udp 设置发送 ip 和端口
	def UdpConnect(self,iFd,sIp,iPort):
		stCmd = SocketServer.UdpSendAddrCmd(SocketServer.CmdType.eCmdUConnect,iFd,self.GetThreadId(),(sIp,iPort))
		self.m_stPartsMgr.m_stSockServer.PushCmd(stCmd)

	#udp 发送数据到指定地址
	#param stUdpAddr 表示一个元组
	def SendTo(self,iFd,sData,stUdpAddr):
		stCmd = SocketServer.SendDataCmd(SocketServer.CmdType.eCmdDataType,iFd,self.GetThreadId(),sData,stUdpAddr)
		self.m_stPartsMgr.m_stSockServer.PushCmd(stCmd)

	#发送数据，tcp udp 都可以发送
	def SendData(self,iFd,sData):
		stCmd = SocketServer.SendDataCmd(SocketServer.CmdType.eCmdDataType,iFd,self.GetThreadId(),sData)
		self.m_stPartsMgr.m_stSockServer.PushCmd(stCmd)

	#关闭一个 socket
	def CloseSock(self,iFd):
		stSockMsg = SocketServer.SockMessage() 
		stSockMsg.m_iFd = iFd
		if not self.CloseCb(stSockMsg):
			logger.error("close error fd=%d"%(iFd))
			return
		stCmd = SocketServer.CloseSockCmd(SocketServer.CmdType.eCmdCloseType,iFd,self.GetThreadId())
		self.m_stPartsMgr.m_stSockServer.PushCmd(stCmd)