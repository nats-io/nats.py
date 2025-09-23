FROM python:3.9
WORKDIR /usr/src/nats.py
ARG PROFILE=test
COPY . /usr/src/nats.py

# Install pipenv
RUN pip install pipenv
RUN pipenv install --dev
RUN pipenv run pytest tests/test_compatibility.py --collect-only -v

ENV NATS_URL=nats://localhost:4222

CMD pipenv run pytest tests/test_compatibility.py -v
