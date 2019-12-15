FROM python:3.7-slim
RUN apt update && apt -y install git \
 && pip install --upgrade pip \
 && pip install git+https://github.com/koji-m/bulq.git
