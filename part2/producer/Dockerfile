FROM python:3.8

WORKDIR /producer

COPY . /producer

RUN pip install -r requirements.txt

CMD [ "python", "-u", "produce.py" ]