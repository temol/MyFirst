FROM temol/python_env_nb:1.2

RUN pip install --no-cache-dir --index-url http://pypi.douban.com/simple --trusted-host pypi.douban.com DBUtils==1.3

COPY *.py /home/

WORKDIR /home
