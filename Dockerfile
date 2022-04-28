FROM python:3.9

WORKDIR /usr/app/src

COPY src/graph_index.py ./
COPY src/requirements.txt ./

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "graph_index.py" ]
