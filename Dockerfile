FROM python:3-slim
WORKDIR /programas/ingesta
RUN pip3 install boto3 pandas mysql.connector
COPY . .
CMD [ "python3", "./ingesta02.py" ]
