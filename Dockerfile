FROM python:3.10

ENV PYTHONUNBUFFERED True

COPY requirements.txt ./

RUN pip install -r requirements.txt

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./
RUN rm -rf ./venv
RUN chmod +x ./script.sh

ENTRYPOINT ["./script.sh"]
