# 一键构建并运行离线演示产品（也可直接部署到 Render / Railway / Fly 等）
FROM python:3.11-slim

WORKDIR /app
COPY 01_可运行Web产品_离线演示/ ./web/

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

# 仅标准库，无需 pip install
CMD ["python", "web/run.py", "--host", "0.0.0.0", "--port", "8000"]
