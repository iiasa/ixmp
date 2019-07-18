FROM openjdk:11
# installing python 3.7
ENV PYTHON_VERSION=3.7.3
RUN apt-get update && apt-get install -y --no-install-recommends build-essential zlib1g-dev liblzma-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget libbz2-dev && \
    curl -O https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz && \
    tar -xf Python-$PYTHON_VERSION.tar.xz && \
    cd Python-$PYTHON_VERSION && \
    ./configure --enable-optimizations && \
    make -j 8 && \
    make altinstall && \
    cd .. && \
    rm -rf Python-$PYTHON_VERSION && \
    apt-get install -y --no-install-recommends python3-pip  && \
    pip3 install virtualenv && \
    virtualenv -p python3.7 /opt/python3
# installing GAMS
ENV GAMS_VERSION=27.3.0
RUN curl -O https://d37drm4t2jghv5.cloudfront.net/distributions/$GAMS_VERSION/linux/linux_x64_64_sfx.exe
RUN unzip linux_x64_64_sfx.exe
ENV GAMS_PATH=/gams27.3_linux_x64_64_sfx
ENV PATH=$PATH:$GAMS_PATH
# installing graphviz
RUN apt-get install -y --no-install-recommends graphviz
