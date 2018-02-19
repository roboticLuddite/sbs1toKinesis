FROM thepatrik/scipy-shapely

RUN python -c "import sys ; print(sys.executable)"


COPY . /sbs1Src/

RUN pip install boto3 

WORKDIR "/sbs1Src/"

RUN ls

ENTRYPOINT ["/usr/local/bin/python", "dockerExec.py" ]

CMD ["sbs1tokinesis", "/kmls/test0.kml", "aws-athena-query-results-786156393318-us-east-1", "sbs1ToIntermediateKml/2017/08/09/83802028-d6e5-4c14-8673-159e583c1950.csv"]


