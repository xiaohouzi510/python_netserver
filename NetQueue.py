#!/usr/bin/python
# encoding=utf-8

import ThreadLock

#-------------本文以单链表方式操作链表------------------
#从头部获得一条消息
def MsgPop(stQueue):
	if stQueue.m_stLock != None:
		stQueue.m_stLock.Lock()

	if stQueue.m_stHead == None:
		if stQueue.m_stLock != None:
			stQueue.m_stLock.UnLock()
		return None 
		
	stResult = stQueue.m_stHead
	stQueue.m_stHead = stQueue.m_stHead.m_stNext

	if stQueue.m_iCount != None:
		stQueue.m_iCount = stQueue.m_iCount - 1 

	if stQueue.m_stLock != None:
		stQueue.m_stLock.UnLock()
	stResult.m_stNext = None
	
	return stResult

#头链表添加结点
def AddHead(stLink,stNode): 
	if stLink.m_stLock != None:
		stLink.m_stLock.Lock()

	if stLink.m_stHead == None:
		stLink.m_stHead = stNode
		stLink.m_stTail = stNode
	else:
		stNode.m_stNext = stLink.m_stHead 
		stLink.m_stHead = stNode

	if stLink.m_iCount != None:
		stLink.m_iCount = stLink.m_iCount + 1
		if stLink.m_iCount >= 100:
			logger.error("msg too large link=%s node=%s"%(stLink,stNode))

	if stLink.m_stLock != None:
		stLink.m_stLock.UnLock()

#尾链表添加结点
def AddTail(stLink,stNode):
	if stLink.m_stLock != None:
		stLink.m_stLock.Lock()

	if stLink.m_stHead == None:
		stLink.m_stHead = stNode
		stLink.m_stTail = stNode
	else:
		stLink.m_stTail.m_stNext = stNode	
		stLink.m_stTail = stNode

	if stLink.m_iCount != None:
		stLink.m_iCount = stLink.m_iCount + 1

	if stLink.m_stLock != None:
		stLink.m_stLock.UnLock()