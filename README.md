### Build Mist jobs

```sh
cd jobs
sbt package
cd ..
```

### Run MQTT server

```sh
docker run docker run --name crimethory-mosquitto -d ansi/mosquitto
```

### Run Mist

Create twitter credential file `configs/twitter4j.properties`
[http://twitter4j.org/en/configuration.html](http://twitter4j.org/en/configuration.html)

```sh
docker run -d --link crimethory-mosquitto:mosquitto -p 2003:2003 --name crimethory-mist -v  $PWD/jobs/target/scala-2.11/:/jobs -v $PWD/configs/:/usr/share/mist/configs -v $PWD/configs/twitter4j.properties:/usr/share/spark/conf/twitter4j.properties -t hydrosphere/mist:master-2.0.0 mist
```

### Run streaming jobs

```sh
docker exec -it crimethory-mist bash -c "/usr/share/mist/bin/mist start job --config /usr/share/mist/configs/docker.conf --route twitter --parameters {}"
```

### Build application

```sh

docker build -t crimethory:latest .

```

### Run application

```sh

docker run -p 3000:3000 -d --name crimethory --link crimethory-mosquitto:mosquitto crimethory:latest npm

```

### Tweet something with [#crimethory](https://twitter.com/search?f=tweets&vertical=default&q=%23crimethory) hashtag
