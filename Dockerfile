FROM python:3.6.4
WORKDIR /code
ENV FLASK_APP app-sleep.py
ENV FLASK_RUN_HOST 0.0.0.0
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["flask", "run"]