FROM openjdk:11-slim-buster

# Install Python and other dependencies
ARG VIRTUAL_ENV=/opt/python3

RUN apt-get update &&\
    apt-get install -y --no-install-recommends curl graphviz python3 \
      python3-distlib python3-pip python3-setuptools unzip &&\
    pip3 install virtualenv &&\
    virtualenv -p python3 $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install GAMS
# NB see also ci/env.sh and try to keep version in sync.
ARG GAMS_VERSION=27.3.0

RUN curl -O https://d37drm4t2jghv5.cloudfront.net/distributions/$GAMS_VERSION/linux/linux_x64_64_sfx.exe &&\
    echo '4fb888092c97053d787fb93566565401 *linux_x64_64_sfx.exe' | md5sum -c &&\
    unzip -q linux_x64_64_sfx.exe &&\
    rm linux_x64_64_sfx.exe

ENV GAMS_PATH=/gams27.3_linux_x64_64_sfx
ENV PATH="$PATH:$GAMS_PATH"
