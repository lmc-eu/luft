FROM ubuntu:bionic


# Embulk variables
ENV EMBULK_VERSION=0.9.19 \
    EMBULK_GEM=' \
      embulk-input-mysql \
      embulk-input-postgresql \
      embulk-input-elasticsearch-nosslverify \
      embulk-output-s3 \
      embulk-output-gcs \
      embulk-output-parquet \
    ' \
    NONEMBULK_GEM=' \
      httpclient \
    '

# Install packages
    # Required packages.
ENV PACKAGE=' \
      git \
      curl \
      nano \
      bash \
      python3 \
      python3-pip \
    ' \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# Instal Java
RUN apt-get update && \
    apt-get -y -o Dpkg::Options::="--force-overwrite" install openjdk-8-jdk && \
    apt-get -y install ${PACKAGE} && \
    apt-get clean && \
    rm -r /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# install embulk
RUN curl -L https://dl.bintray.com/embulk/maven/embulk-${EMBULK_VERSION}.jar -o /opt/embulk/embulk.jar --create-dirs -k && \
    chmod +x -R /opt/embulk

# add embulk to PATH
ENV PATH $PATH:/opt/embulk

# install embulk gems
RUN if [ "$EMBULK_GEM" != "" ]; then embulk.jar gem install ${EMBULK_GEM}; fi
RUN if [ "$NONEMBULK_GEM" != "" ]; then embulk.jar gem install ${NONEMBULK_GEM}; fi

# Todo Get rid of this after publishing ot Pypi
ADD . /work
WORKDIR /work
# Install luft
RUN pip3 install -e ".[bq]"
ENTRYPOINT ["luft"]
CMD ["--help"]