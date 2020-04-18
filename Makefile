init:
	pip install -r requirements.txt

test:
	py.test tests

freeze:
	# See https://stackoverflow.com/questions/39577984/what-is-pkg-resources-0-0-0-in-output-of-pip-freeze-command
	pip freeze | grep -v "pkg-resources" > requirements.txt

.PHONY: init test
