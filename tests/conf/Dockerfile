FROM python:3.9
WORKDIR /usr/src/nats.py
ARG PROFILE=test
COPY . /usr/src/nats.py

# Install dependencies
RUN pipenv install --dev

RUN pytest --collect-only

ENV NATS_URL=localhost:4222

CMD pytest tests/test_compatibility.py -v
