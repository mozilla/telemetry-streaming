rm -rf /tmp/checkpoint /tmp/parquet
EN=$(ifconfig en0 | grep "inet " | grep -oE '([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)' | head -n 1)
ETH=$(ifconfig eth0 | grep "inet " | grep -oE '([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)' | head -n 1)

if test -z "$EN"; then
    DOCKER_KAFKA_HOST=$ETH
else
    DOCKER_KAFKA_HOST=$EN
fi

echo $DOCKER_KAFKA_HOST
