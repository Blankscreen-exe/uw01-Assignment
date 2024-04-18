
## Installation

1. Run the docker Redis Service

```sh
docker compose up
```

2. access redis commander on `http://127.0.0.1:8081`

3. create virtual env and install dependencies

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
