install:
	poetry install 

lint:
	poetry run ruff check --fix . 

format: 
	poetry run ruff format . 

test:
	poetry run pytest tests/unit --cov 

validate:
	databricks bundle validate 

deploy_dev:
	databricks bundle deploy -t dev 

deploy:
	databricks bundle deploy -t prod 