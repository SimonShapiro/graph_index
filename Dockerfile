FROM python:3.9

WORKDIR /usr/app/src

COPY src/graph_index.py /usr/app/src
COPY src/requirements.txt /usr/app/src

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "graph_index.py" ]
#CMD /bin/bash