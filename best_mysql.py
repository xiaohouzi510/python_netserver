#!/usr/bin/python
# encoding=utf-8

import MsgQueue
import best_consumer 
import logging

logger = logging.getLogger(__name__)

#------入库类-------
class best_mysql:
	def __init__(self,parts_mgr):
		self.m_parts_mgr = parts_mgr
		self.m_consumer  = best_consumer.best_consumer()
		parts_mgr.m_stMsgQ.RegMsg(MsgQueue.MsgData(MsgQueue.MsgQType.eMsgServer,self.server_dispathch_msg))

	#server 消息
	def server_dispathch_msg(self,server_msg):
		#server_msg = MsgQueue.ServerMessage
		status,error_str = self.m_consumer.run(server_msg.m_sData)
		if not status:
			logger.error("%s"%error_str)
			print(error_str)
			return False
		return True

	#初始化 consumer
	def init_consumer(self):
		self.m_consumer.init_table()
		self.m_consumer.init_check()
		status,error_str = self.init_set_point_table()	
		if not status:
			logger.error("%s"%error_str)
		status,error_str = self.init_device_table()	
		if not status:
			logger.error("%s"%error_str)
		status,error_str = self.m_consumer.init()	
		if not status:
			logger.error("%s"%error_str)
		return True

	#step_point_ 前缀的表
	def init_set_point_table(self):
		db_file 	     = "best_client_db.json"
		table_describe   = "best_step_point_table_describe.json"
		table_prefix     = "step_point_"
		etype 		     = "step_point"	
		status,error_str = self.m_consumer.add_config(etype,db_file,table_describe,table_prefix)
		if not status:
			return status,error_str
		status,error_str = self.m_consumer.add_field_customize_fun(etype,"create_time",self.m_consumer.get_time_statmp)
		if not status:
			return status,error_str
		status,error_str = self.m_consumer.add_field_customize_fun(etype,"os",self.m_consumer.os_change)
		if not status:
			return status,error_str
		return True,"OK"

	#step_point_ 前缀的表
	def init_device_table(self):
		db_file 	     = "best_client_db.json"
		table_describe   = "best_device_table_describe.json"
		table_prefix     = "device_"
		etype 		     = "device"	
		status,error_str = self.m_consumer.add_config(etype,db_file,table_describe,table_prefix)
		if not status:
			return status,error_str 
		status,error_str = self.m_consumer.add_field_customize_fun(etype,"create_time",self.m_consumer.get_time_statmp)
		if not status:
			return status,error_str 
		return True,"OK"