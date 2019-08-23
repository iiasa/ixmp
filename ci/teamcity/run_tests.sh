#!/usr/bin/env bash

finddir() {
  if [ -f "$1" ]; then
    printf '%s\n' "${PWD%/}"
  elif [ "$PWD" = / ]; then
    false
  else
    (cd .. && finddir "$1")
  fi
}

PROJECT_ROOT=$(finddir setup.py)
IMAGE_NAME=ene-docker.iiasa.ac.at/java-python-base

docker build -t ${IMAGE_NAME} --file ${PROJECT_ROOT}/ci/teamcity/Dockerfile ${PROJECT_ROOT}

docker run -it --rm -v ${PROJECT_ROOT}:/work -w /work ${IMAGE_NAME} \
  bash -c "pip install teamcity-messages && pip install .[tests] && py.test tests"