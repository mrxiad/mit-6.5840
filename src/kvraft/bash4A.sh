for i in {1..10}; do echo "Running 4A test iteration $i..."; go test -run 4A -race; if [ $? -ne 0 ]; then echo "Test iteration $i failed!"; exit 1; fi; done
