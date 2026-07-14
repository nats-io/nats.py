FROM python:3.9
WORKDIR /usr/src/nats.py
ARG PROFILE=test
COPY . /usr/src/nats.py

# Install uv
RUN pip install uv
RUN uv sync --dev
RUN uv run pytest nats/tests/test_compatibility.py --collect-only -v

ENV NATS_URL=nats://localhost:4222

CMD uv run pytest nats/tests/test_compatibility.py -v
