FROM python:3-slim
WORKDIR /programas/ingesta
RUN pip3 install boto3 pandas
COPY . .
CMD ["python3", "ingesta_songs.py"]