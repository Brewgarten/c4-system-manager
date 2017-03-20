FROM jfloff/alpine-python:2.7

# install tools
RUN apk --no-cache --update add curl tar

# setup Python development environment
RUN pip install --disable-pip-version-check --no-cache-dir \
    coverage==4.3 \
    pylint==1.6 \
    pytest==3.0 \
    pytest-cov==2.4 \
    virtualenv==15.1

# install etcd
ENV ETCD_VERSION v3.1.3
ENV ETCD_DOWNLOAD_URL https://github.com/coreos/etcd/releases/download
RUN curl -L ${ETCD_DOWNLOAD_URL}/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VERSION}-linux-amd64.tar.gz && \
    mkdir -p /opt/etcd && tar xzvf /tmp/etcd-${ETCD_VERSION}-linux-amd64.tar.gz -C /opt/etcd --strip-components=1
