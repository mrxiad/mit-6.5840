#!/bin/bash
for i in {1..10}
do
  echo "Running test iteration $i..."
  go test -run 3C -race
  if [ $? -ne 0 ]; then
    echo "Test iteration $i failed!"
    exit 1
  fi
done

