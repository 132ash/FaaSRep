FROM workflow_sub

# 拷贝代码文件
COPY proxy.py /proxy/proxy.py
COPY main.py /proxy/main.py
COPY Store.py /proxy/Store.py
COPY beldi_store.py /proxy/beldi_store.py
COPY container_config.py /proxy/container_config.py

# 设置工作目录
WORKDIR /proxy

# 暴露端口
EXPOSE 5000

# 设置启动命令
CMD ["python3", "/proxy/proxy.py"]