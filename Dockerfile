FROM python:3.9-buster
ENV PYTHONUNBUFFERED=1
COPY poetry.lock pyproject.toml collector.py .env ./
RUN pip install -U pip poetry
RUN poetry config virtualenvs.create false && poetry install
CMD python collector.py BTC_JPY
