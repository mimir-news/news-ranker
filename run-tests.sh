#!/bin/bash

TARGET_FOLDERS=(
    "./cmd/"
    "./pkg/domain/"
    "./pkg/repository/"
)

test_failed=false

for target in "${TARGET_FOLDERS[@]}"
do
    go test $target
    test_success=$(echo $?)
    if [ "$test_success" = 1 ]; then
        test_failed=true
    fi
done 

if [ $test_failed = true ]; then
    echo "FAIL"
    exit 1
fi

echo "OK"
exit 0