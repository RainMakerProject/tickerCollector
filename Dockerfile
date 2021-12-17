FROM python:3.9-buster
ENV PYTHONUNBUFFERED=1
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install
COPY poetry.lock pyproject.toml collector.py .env ./
COPY library/ ./library/
RUN pip install -U pip poetry
RUN poetry config virtualenvs.create false && poetry install --no-dev
CMD python collector.py BTC_JPY FX_BTC_JPY
