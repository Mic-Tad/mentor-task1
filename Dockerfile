FROM python:3.10 

WORKDIR /app

COPY . .

RUN pip install -r requirments.txt
CMD ["python","main.py"]