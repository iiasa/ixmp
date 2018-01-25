FROM gidden/messageix-base

COPY . /ixmp
WORKDIR /
ENV MESSAGE_IX_PATH /ixmp
ENV IXMP_R_PATH /ixmp/ixmp
RUN cd /ixmp && python2 setup.py install 
RUN cd /ixmp && python3 setup.py install
