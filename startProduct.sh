#!/usr/bin/env bash
#/bin/bash
file_content=$( cat best_test.txt )
url="127.0.0.1:8888"
for((i=1;i<=10000;++i))
{
	curl -d "$file_content" $url/best
}
