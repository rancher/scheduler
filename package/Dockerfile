FROM ubuntu:16.04
RUN apt-get update && \
    apt-get install -y ca-certificates
COPY scheduler /usr/bin/
CMD ["scheduler"]
