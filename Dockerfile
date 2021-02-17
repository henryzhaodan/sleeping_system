FROM python:3.6.4
WORKDIR /code
ENV FLASK_APP app-sleep.py
ENV FLASK_RUN_HOST 0.0.0.0
RUN apk add --no-cache gcc musl-dev linux-headers
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["flask", "run"]