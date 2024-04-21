clear:
	find . \( -type d -name '__pycache__' -o -name '.mypy_cache' -o -name '.pytest_cache' \) -exec rm -rf {} +