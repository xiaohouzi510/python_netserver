#!/usr/bin/python
# encoding=utf-8

import TimeMgr
import NetQueue
import logging
import ThreadLock

logger = logging.getLogger(__name__)

#所有的 msg queue
g_stMsgQ = {}

def AddMsgQ(iId,stQ):
	g_stMsgQ[iId] = stQ

#消息类型
class MsgQType:
	#网络消息
	eMsgNetSock  = 1
	#定时器消息
	eMsgTimer    = 2

#消息体
class PressMessage:
	def __init__(self):
		self.m_eType  = 0 	 #MsgQType
		self.m_stData = None
		self.m_stNext = None

#应答消息
class ResMessage:
	def __init__(self):
		#会话 ID
		self.m_iSession = 0
		#工作线程 ID
		self.m_iId 		= 0	
		#回调后是否移除
		self.m_bLoop    = False

#--------消息队列(数据结构为链表，非数组)---------
class MessageQueue:
	def __init__(self):
		self.m_stLock    = ThreadLock.ThreadLock()
		#队头，PressMessage
		self.m_stHead    = None
		#队尾
		self.m_stTail    = None
		#个数
		self.m_iCount 	 = 0 

#NetServer 有网络消息
def ForwardSockMsg(stSockMsg):
	#stMsg = SockMessage 
	stQ   = g_stMsgQ[stSockMsg.m_iId]
	stMsg = PressMessage()	
	stMsg.m_eType  = MsgQType.eMsgNetSock
	stMsg.m_stData = stSockMsg 
	stQ.PushMsg(stMsg)	

#定时器消息
#param stTimeNode = TimeMgr.TimeNode 
def ForwardTimerMsg(stTimeNode):
	iId = stTimeNode.m_iId
	#定时器采用多线程线并且对 stTimeNode 结点回收，
	#在回调期间可能会被重置掉，如果 id 为 0，在回调时会被丢掉
	if iId == 0:
		logger.error("timer dispatch id zero node=%s"%stTimeNode)
		return
	stQ   = g_stMsgQ[iId]
	stMsg = PressMessage()	
	stMsg.m_eType  = MsgQType.eMsgTimer
	stMsg.m_stData = stTimeNode 
	stQ.PushMsg(stMsg)

#请求应答数据
class ResData:
	def __init__(self,fCb,stCbData):
		self.m_fCb 		= fCb
		self.m_stCbData = stCbData

#消息回调，需注册
class MsgData:
	def __init__(self,eType,fDispatch):
		#eType = MsgQType
		self.m_eType     = eType
		self.m_fDispatch = fDispatch

#定时器回收链表
class GCTimeLink:
	def __init__(self):
		self.m_stLock    = None 
		self.m_stHead    = None
		self.m_stTail    = None
		self.m_iCount 	 = 0 

#消息调度管理
class MsgQueue:
	def __init__(self,stPartsMgr):
		#部件
		self.m_stPartsMgr   = stPartsMgr
		#消息类型数据 MsgData
		self.m_stMsgData    = {}
		#消息队列 
		self.m_stMsgMq 		= MessageQueue()
		#回应消息数据
		self.m_hResData     = {}
		#session
		self.m_iSession     = 0
		#定时器结点回收链表
		self.m_stTimeGCLink = GCTimeLink()
		#加入一个 msg queue
		AddMsgQ(self.GetThreadId(),self)
		#注册定时器回调
		self.RegMsg(MsgData(MsgQType.eMsgTimer,self.TimerDispatchMsg))

	#生成一个 session
	def MakeSession(self):
		iRSession = 0
		while True:
			#0不使用
			if self.m_iSession == 0:
				self.m_iSession = 1
			iRSession  = self.m_iSession
			#最大为 4294967295
			self.m_iSession = (self.m_iSession + 1)&TimeMgr.TICK_MASK
			if self.m_hResData.has_key(iRSession) == False:
				break
		return iRSession

	#用于回应类型数据
	def Call(self,fCb,stCbData):
		stRes    = ResData(fCb,stCbData)
		iSession = self.MakeSession()
		self.m_hResData[iSession] = stRes
		return iSession

	#获得线程 ID
	def GetThreadId(self):
		return self.m_stPartsMgr.m_stThread.GetThreadId()

	#获得一个定时器结点
	def GetTimerNode(self):
		stTimeNode = NetQueue.MsgPop(self.m_stTimeGCLink)
		if stTimeNode != None:
			return stTimeNode
		return TimeMgr.TimeNode() 

	#释放一个定时器结点
	def ReleaseTimerNode(self,stTimeNode,fCb):
		#TimeMgr.RemoveTimer 有可能返回 None 结点
		if stTimeNode == None:
			return
		logger.debug("release timer %s cb=%s node=%s"%(stTimeNode.Log(),fCb,stTimeNode))
		stTimeNode.Release()
		NetQueue.AddTail(self.m_stTimeGCLink,stTimeNode)	

	#添加定时器
	def AddTimer(self,fCb,iTime,bLoop,stData):
		stTimeNode = self.GetTimerNode()
		iSession   = self.Call(fCb,None)
		#外部调用定时器提供
		stTimeNode.m_bLoop    = bLoop
		stTimeNode.m_iTime    = iTime
		stTimeNode.m_iId      = self.GetThreadId()
		stTimeNode.m_iSession = iSession
		stTimeNode.m_stData   = stData

		#定时器内部使用
		stTimeNode.m_stFather = None
		stTimeNode.m_iExpire  = 0 	
		stTimeNode.m_stNext   = None
		stTimeNode.m_stFront  = None
		self.m_stPartsMgr.m_stTimerMgr.AddTimer(stTimeNode)
		logger.debug("add timer session=%d"%(iSession))
		return iSession

	#删除定时器
	def RemoveTimer(self,iSession):
		if not self.m_hResData.has_key(iSession):
			logger.error("not found timer session=%d"%iSession)
			return
		stData = self.m_hResData[iSession]
		del self.m_hResData[iSession]
		stTimeNode =self.m_stPartsMgr.m_stTimerMgr.RemoveTimer(self.GetThreadId(),iSession)
		self.ReleaseTimerNode(stTimeNode,stData.m_fCb)

	#定时器回调
	def TimerDispatchMsg(self,stTimeNode):
		#stTimeNode = TimeMgr.TimeNode 
		#定时器有可能返回时被取消了
		if not self.m_hResData.has_key(stTimeNode.m_iSession):
			logger.warning("timer res not found session=%d node=%s"%(stTimeNode.m_iSession,stTimeNode))
			return True
		stData = self.m_hResData[stTimeNode.m_iSession]
		stData.m_fCb(stTimeNode.m_stData)
		#回调时可能把定时器删除掉
		if stTimeNode.m_iSession == 0:
			logger.warning("loop timer delete when call back node=%s"%stTimeNode)
			return
		if not stTimeNode.m_bLoop:
			del self.m_hResData[stTimeNode.m_iSession]
			self.ReleaseTimerNode(stTimeNode,stData.m_fCb)

	#注册接收到消息时所需数据
	#param stMsgData=MsgData
	def RegMsg(self,stMsgData):
		if self.m_stMsgData.has_key(stMsgData.m_eType):
			logger.error("double reg msg data type=%d"%(stMsgData.m_eType))
			return
		self.m_stMsgData[stMsgData.m_eType] = stMsgData		

	#添加消息
	def PushMsg(self,stNode):	
		NetQueue.AddTail(self.m_stMsgMq,stNode)

	#循环
	def MsgLoop(self):
		stMsg = NetQueue.MsgPop(self.m_stMsgMq)
		if stMsg == None:
			return False 
		self.m_stMsgData[stMsg.m_eType].m_fDispatch(stMsg.m_stData)
		return True