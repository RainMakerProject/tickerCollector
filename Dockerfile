FROM python:3.9-buster
ENV PYTHONUNBUFFERED=1
COPY poetry.lock pyproject.toml collector.py .env ./
COPY library/ ./library/
RUN pip install -U pip poetry
RUN poetry config virtualenvs.create false && poetry install --no-dev
CMD python collector.py BTC_JPY FX_BTC_JPY
