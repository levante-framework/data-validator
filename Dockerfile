FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install setuptools with pkg_resources before other deps (setuptools 82+ removed it).
COPY requirements.txt .
RUN pip install --no-cache-dir 'setuptools>=69,<82' wheel \
    && pip install --no-cache-dir -r requirements.txt \
    && python -c "import pkg_resources; import redivis"

COPY . .

CMD ["python", "main.py"]
