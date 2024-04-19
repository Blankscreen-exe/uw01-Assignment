
## Installation

1. create virtual env and install dependencies

```sh
pip3 install -r requirements.txt
```

3. copy and edit `.env` file

4. create a `temp` folder in the root of the project

5. Run the application

```sh
uvicorn main:app --reload
```

6. go to `http://127.0.0.1:8000/docs` for Swagger UI

### Setup Redis (docker)

1. Run the docker Redis Service

```sh
docker compose up
```

2. access redis commander on `http://127.0.0.1:8081`
3. your redis service will be running at port `6379`

### Setup Redis (locally)

1. Install `redis-cli`. Follow [this guide](https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/) if you face any problems

```sh
sudo apt-get install redis
```

2. Check if `redis` is corrently installed

```sh
redis-cli
```

3. Install `redis-commander`. Note that you need `npm` before you can install `redis-commander`

```sh
npm install -g redis-commander
```

4. Start `redis-commander` service. Probably on port `8081`

```sh
redis-commander
```