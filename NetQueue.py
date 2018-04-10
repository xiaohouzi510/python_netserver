#!/usr/bin/python
# encoding=utf-8

import ThreadLock

#从头部获得一条消息
def MsgPop(stQueue):
	#有锁则加锁
	if hasattr(stQueue,"m_stLock") and stQueue.m_stLock != None:
		stQueue.m_stLock.Lock()

	if stQueue.m_stHead == None:
		if hasattr(stQueue,"m_stLock") and stQueue.m_stLock != None:
			stQueue.m_stLock.UnLock()
		return None 
		
	stResult = stQueue.m_stHead
	stQueue.m_stHead = stQueue.m_stHead.m_stNext

	if stQueue.m_iCount != None:
		stQueue.m_iCount = stQueue.m_iCount - 1 

	if hasattr(stQueue,"m_stLock") and stQueue.m_stLock != None:
		stQueue.m_stLock.UnLock()
	stResult.m_stNext = None
	#有 m_stFront 属性则置空
	if hasattr(stResult,"m_stFront"):
		stResult.m_stFront = None
	
	return stResult

#头链表添加结点
def AddHead(stLink,stNode): 
	if hasattr(stLink,"m_stLock") and stLink.m_stLock != None:
		stLink.m_stLock.Lock()

	if stLink.m_stHead == None:
		stLink.m_stHead = stNode
		stLink.m_stTail = stNode
	else:
		if hasattr(stNode,"m_stFront"):
			stLink.m_stHead.m_stFront = stNode 
		stNode.m_stNext = stLink.m_stHead 
		stLink.m_stHead = stNode

	if hasattr(stLink,"m_iCount") and stLink.m_iCount != None:
		stLink.m_iCount = stLink.m_iCount + 1
		if stLink.m_iCount >= 100:
			logger.error("msg too large link=%s node=%s"%(stLink,stNode))

	if hasattr(stLink,"m_stLock") and stLink.m_stLock != None:
		stLink.m_stLock.UnLock()

#尾链表添加结点
def AddTail(stLink,stNode):
	if hasattr(stLink,"m_stLock") and stLink.m_stLock != None:
		stLink.m_stLock.Lock()

	if stLink.m_stHead == None:
		stLink.m_stHead = stNode
		stLink.m_stTail = stNode
	else:
		if hasattr(stNode,"m_stFront"):
			stNode.m_stFront = stLink.m_stTail
		stLink.m_stTail.m_stNext = stNode	
		stLink.m_stTail = stNode

	if hasattr(stNode,"m_stLink"):
		stNode.m_stLink = stLink

	if hasattr(stLink,"m_iCount") and stLink.m_iCount != None:
		stLink.m_iCount = stLink.m_iCount + 1

	if hasattr(stLink,"m_stLock") and stLink.m_stLock != None:
		stLink.m_stLock.UnLock()

#移除一个结点，只能用于双向链表，结点中要包含 m_stLink，即为链表
def RemoveNode(stNode):
	if not hasattr(stNode,"m_stLink") or stNode.m_stLink == None:
		return
	stLink = stNode.m_stLink
	if stNode.m_stFront != None:
		stNode.m_stFront.m_stNext = stNode.m_stNext
	else:
		stLink.m_stHead = stNode.m_stNext

	if stNode.m_stNext != None:
		stNode.m_stNext.m_stFront = stNode.m_stFront
	else:
		stLink.m_stTail = stNode.m_stFront	
	stNode.m_stFront = None
	stNode.m_stNext  = None
	stNode.m_stLink  = None
	if hasattr(stLink,"m_iCount") and stLink.m_iCount != None:
		stLink.m_iCount = stLink.m_iCount - 1
	#m_stHead 为 None，表示该链表为空
	if stLink.m_stHead == None:
		RemoveNode(stLink)