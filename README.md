### Build container

```sh

docker build -t crimethory:latest .

```

### Run container

```sh

docker run -p 3000:3000 -d --name crimethory crimethory:latest npm

```
