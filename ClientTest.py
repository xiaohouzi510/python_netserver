#!/usr/bin/python
# encoding=utf-8

import socket
import sys
import struct
import re

#打包一个数据
def PackData(sData):
	sSendData  = struct.pack('!i',len(sData))
	sSendData += sData
	return sSendData	

#解一个包
def UpackData(sData):
	return sData[4:]	

#主函数
if __name__ == '__main__':
	stSock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	stSock.connect(("127.0.0.1",8000))	
	stSelfAddr = stSock.getsockname()
	stPeerAddr = stSock.getpeername()
	print("connect success server=%s client=%s"%(stPeerAddr,stSelfAddr))
	while True:
		sys.stdout.write(">")
		sData = sys.stdin.readline()
		iLen  = len(sData)
		if iLen == 1:
			continue
		sData = sData[0:iLen-1]
		sData = PackData(sData)
		iLen  = len(sData)
		stSock.send(sData)
		sData = stSock.recv(iLen)
		print(">recv data=%s"%(UpackData(sData)))