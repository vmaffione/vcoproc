#!/bin/sh

for i in $(seq 1 80); do
	for j in $(seq 5 58); do
		mkdir -p in/${i}/.${j}
		BN=$(uuidgen)
		truncate -s 500K in/${i}/.${j}/${BN}_1.wav
		truncate -s 1M in/${i}/.${j}/${BN}_2.wav
		echo '{"xyz": "aisoucasidcusaicdn", "oiq": 3}' > in/${i}/.${j}/metadata.json
		mv in/${i}/.${j} in/${i}/${j}
	done
done
