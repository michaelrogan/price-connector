# Market price MCP connector (Yahoo chart + Stooq) — for public hosting.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
ENV MCP_TRANSPORT=http HOST=0.0.0.0
# No API key needed. Optional:
#   PRICE_USER_AGENT="..."   PRICE_MAX_RPS="2"
# PORT is injected by the host; the server binds to it automatically.
CMD ["python", "server.py"]
