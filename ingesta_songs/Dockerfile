FROM python:3-slim
WORKDIR /programas/ingesta
RUN mkdir /programas/ingesta/logs_output
RUN pip3 install boto3 pandas loguru
COPY . .
CMD ["python3", "ingesta_songs.py"]