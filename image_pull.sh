#!/bin/bash
# If you can't execute this file
# Please run " chmod +x <path of this file> "

echo "Start pulling PARSEC container images"

echo "Pulling splash2x.fft image"
sudo docker pull anakli/parsec:splash2x-fft-native-reduced

echo "Pulling freqmine image"
sudo docker pull anakli/parsec:freqmine-native-reduced

echo "Pulling ferret image"
sudo docker pull anakli/parsec:ferret-native-reduced

echo "Pulling canneal image"
sudo docker pull anakli/parsec:canneal-native-reduced

echo "Pulling dedup image"
sudo docker pull anakli/parsec:dedup-native-reduced

echo "Pulling blackscholes"
sudo docker pull anakli/parsec:blackscholes-native-reduced






