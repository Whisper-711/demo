FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 安装必要的包
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制爬虫代码
COPY newest.py .

# 创建数据目录
RUN mkdir -p /app/ati_surcharge_data

# 设置数据卷
VOLUME /app/ati_surcharge_data

# 运行爬虫
CMD ["python", "newest.py"]
