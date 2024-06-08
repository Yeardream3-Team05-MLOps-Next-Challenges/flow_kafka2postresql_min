FROM prefecthq/prefect:2.18.3-python3.10

# Define build-time variable

COPY requirements.txt .

RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

CMD ["python", "./flow/flow.py"]