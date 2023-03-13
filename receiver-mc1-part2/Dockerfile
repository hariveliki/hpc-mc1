FROM python:3.8

WORKDIR /receiver

COPY . /receiver

RUN pip install -r requirements.txt

CMD [ "python", "-u", "receive.py" ]