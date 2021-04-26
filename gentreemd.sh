#!/bin/sh

for i in $(seq 1 20); do
	for j in $(seq 5 18); do
		mkdir -p in/${i}/${j}
		BN=$(uuidgen)
		touch in/${i}/${j}/${BN}_1.wav
		touch in/${i}/${j}/${BN}_2.wav
		echo '{"xyz": "aisoucasidcusaicdn", "oiq": 3}' > in/${i}/${j}/metadata.json
	done
done
