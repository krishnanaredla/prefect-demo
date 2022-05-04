# Sample Prefect Flow

Following is an example for a sample prefect flow

## Prefect Installation

Prefect documentation can be found her [prefect](https://docs.prefect.io/)

for this tutorial we are using prefect local server 

Installtion

```python
$ pip install prefect
```

To use Prefect Server as the backend, run the following command to configure Prefect for local orchestration:

```python
$ prefect backend server
```

Please note the server requires Docker and Docker Compose to be running.

To start the server, UI, and all required infrastructure, run:

```python
$ prefect server start
```
When server is running, navigate to http://localhost:8080

## Diabetic Prediction Model

First we need to register a project in prefect

```python
$ prefect create project sample
```

We need to register our flow

```python
$ prefect register --project "sample" --path diabetic/flows.py
```

we can also use the regsiter.py to register flow

```python
$ python ./register.py
```

Run the flow

use the customised pipeline.py for programmatic running of the flow or use prefect cli

```python
$ python ./pipeline.py
```

```python
$ prefect run --project "sample"  --name "diabetic" --watch
```

Use prefect UI to monitor the flow

### Documentation

[mkdocs](https://www.mkdocs.org/) is used in for documentation

* Note : prefect flows can be visualized locally , for this graphviz needs to be installed and additional prefect dependencies (prefect[viz]) from pip must installed

We use github pages to host the documentation of this project

```
$ mkdocs gh-deploy
```
 Above will deploy gh pages 