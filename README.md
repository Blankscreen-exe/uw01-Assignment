
1. Run the docker Redis Service
```
docker compose up
```

2. create virtual env and instal dependencies
```
pip install -r requirements.txt
```

3. copy and edit `.env` file

4. create a `temp` folder in the root of the project

5. Run the application
```
 uvicorn main:app --reload
```

6. go to `http://127.0.0.1:8000/docs` for Swagger UI


