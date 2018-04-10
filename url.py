#!/usr/bin/python
# encoding=utf-8

import re

#十六进制转为字符
def decode_fun(obj):
	s_str   = obj.group()
	hex_str = s_str[1:]
	dec_num = int(hex_str,16)
	return chr(dec_num)	

#+ 号转为空格，十六进制转成字符
def decode(s_str):
	#32 为空格
	s_str = re.sub("\+",chr(32),s_str)
	return re.sub("%..",decode_fun,s_str)

#将请求数据转成键值对
def parse_query(s_str):
	pattern = re.compile("([^=]*)=([^&]*)&?")
	array = pattern.findall(s_str)
	result = {}
	for v in array:
		result[decode(v[0])] = decode(v[1])
	return result 