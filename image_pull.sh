#!/bin/bash
# If you can't execute this file
# Please run " chmod +x <path of this file> "

echo "Start pulling PARSEC container images"

echo "Pulling splash2x.fft image"
docker pull anakli/parsec:splash2x-fft-native-reduced

echo "Pulling freqmine image"
docker pull anakli/parsec:freqmine-native-reduced

echo "Pulling ferret image"
docker pull anakli/parsec:ferret-native-reduced

echo "Pulling canneal image"
docker pull anakli/parsec:canneal-native-reduced

echo "Pulling dedup image"
docker pull anakli/parsec:dedup-native-reduced

echo "Pulling blackscholes"
docker pull anakli/parsec:blackscholes-native-reduced






