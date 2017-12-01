#!/usr/bin/env bash

if output=$(git status --porcelain) && [ -z "$output" ]; then
    ## -- get the current sequence
    SEQ=$(cat ./sequence)

    ## -- calculate the next sequence
    let "SEQ=SEQ+1"

    echo "${SEQ}" > ./sequence

    git commit -a -m "Releasing version ${SEQ}"
    git tag "release-${SEQ}"
    git push --tags
    git push

    echo "Deploying version ${SEQ}"

    echo "BUILDING THE DOCKER IMAGE AS LATEST"
    docker build -t dataprism/dataprism-sync-runtime:${SEQ} .

    echo "TAGGING THE DOCKER IMAGE WITH SEQUENCE ${SEQ}"
    docker tag dataprism/dataprism-sync-runtime:${SEQ} dataprism/dataprism-sync-runtime:latest

    echo "PUSHING THE DOCKER IMAGE"
    docker push dataprism/dataprism-sync-runtime:${SEQ}
    docker push dataprism/dataprism-sync-runtime:latest

else
  echo "Please commit everything before releasing"
  exit 1
fi

