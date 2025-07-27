# Religious Questions Bot API

This is a simple API for a religious questions bot.

## Running the API

To run the API, you can use uvicorn:

```bash
uvicorn app.main:app --reload
```

## Running the Worker

The worker is responsible for processing active quizzes. To run the worker, you need to set the `PYTHONPATH` to the root of the project and then run the `worker.py` file.

```bash
export PYTHONPATH=$(pwd)
python app/worker.py
```
