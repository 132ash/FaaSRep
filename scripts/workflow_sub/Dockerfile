FROM python:3.7-slim

# 创建工作目录
RUN mkdir /proxy

# 设置工作目录
WORKDIR /proxy

# 安装必要的依赖库
RUN apt-get clean && \
    apt-get update && \
    pip3 install --no-cache-dir gevent flask boto3 redis requests botocore