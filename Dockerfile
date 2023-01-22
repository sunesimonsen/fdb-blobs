# ARG needs to be defined for both FROM instructions,
# see https://github.com/moby/moby/issues/34129
ARG FDB_VERSION
FROM foundationdb/foundationdb:${FDB_VERSION} as fdb
FROM golang:1.19.4-buster
ARG FDB_VERSION

WORKDIR /tmp

RUN apt-get update
# dnsutils is needed to have dig installed to create cluster file
RUN apt-get install -y --no-install-recommends ca-certificates dnsutils

RUN wget https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb
RUN dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb

COPY --from=fdb /var/fdb/scripts/fdb.bash /

WORKDIR /go/src/app
COPY . .

COPY run.bash /run.bash
RUN chmod a+x /run.bash

RUN go get -d -v ./...
RUN go install -v ./...

CMD ["/run.bash"]
