FROM python:3.10.7-slim-buster

RUN mkdir -p /usr/src/temp
WORKDIR /usr/src/temp

RUN pip install --upgrade pip
COPY requirements.txt /usr/src/temp/requirements.txt
RUN pip install -r requirements.txt

RUN apt update \
  && apt install -y curl \
  && apt install -y p7zip-full \
  && apt-get install -y postgresql-client-11


ENV DB_DATA_YEAR1=2020
ENV DB_DATA_YEAR2=2019

RUN curl -O https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO${DB_DATA_YEAR1}.7z
RUN curl -O https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO${DB_DATA_YEAR2}.7z
RUN 7z x OpenDataZNO${DB_DATA_YEAR1}.7z
RUN 7z x OpenDataZNO${DB_DATA_YEAR2}.7z

COPY . /usr/src/temp/

CMD ["python", "main.py"]
