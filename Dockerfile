FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install setuptools with pkg_resources before other deps (setuptools 82+ removed it).
COPY requirements.txt .
# redivis pulls niquests -> urllib3-future, whose .pth replaces site-packages/urllib3
# with its own fork at interpreter startup. That fork breaks google-cloud-storage
# uploads with "Connection aborted / OSError(22)", so drop the .pth and restore genuine
# urllib3; niquests keeps working off the separate urllib3_future package.
RUN pip install --no-cache-dir 'setuptools>=69,<82' wheel \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -f /usr/local/lib/python3.12/site-packages/urllib3_future.pth \
    && rm -rf /usr/local/lib/python3.12/site-packages/urllib3 \
    && pip install --no-cache-dir --force-reinstall --no-deps -c requirements.txt urllib3 \
    && python -c "import pkg_resources, redivis, urllib3, importlib.metadata as m; \
       assert urllib3.__version__ == m.version('urllib3'), \
       f\"urllib3 overridden: module {urllib3.__version__} != dist {m.version('urllib3')}\""

COPY . .

CMD ["python", "main.py"]
