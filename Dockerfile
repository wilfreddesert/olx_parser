FROM python:3.10

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY . /app

EXPOSE 8080

CMD ["uvicorn", "server:app",  "--host", "0.0.0.0", "--port", "8080"]
