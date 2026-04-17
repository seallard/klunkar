FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN mkdir -p klunkar && touch klunkar/__init__.py \
    && pip install --no-cache-dir -e . \
    && rm -rf klunkar
COPY . .
RUN pip install --no-cache-dir -e . --no-deps
CMD ["klunkar", "bot"]
