FROM workflow_sub

# 拷贝代码文件
COPY proxy.py /proxy/proxy.py
COPY main.py /proxy/main.py
COPY Store.py /proxy/Store.py
COPY redis_component.py /proxy/redis_component.py
COPY container_config.py /proxy/container_config.py
COPY shadow_client.py /proxy/shadow_client.py
COPY transaction_errors.py /proxy/transaction_errors.py

# 设置工作目录
WORKDIR /proxy

# 暴露端口
EXPOSE 5000

# 设置启动命令
CMD ["python3", "/proxy/proxy.py"]
