config_py/logging_constants.py:
	cp ./config_py/logging_constants.py.default ./config_py/logging_constants.py

init: config_py/logging_constants.py
	#cd ..
	#python3 -m venv outlet
	#cd outlet
	#./bin/activate
	
	python3 -m pip install -r requirements.txt

test:
	py.test tests

clean:
	rm -rf venv include lib lib64 config_py/logging_constants.py

freeze:
	# See https://stackoverflow.com/questions/39577984/what-is-pkg-resources-0-0-0-in-output-of-pip-freeze-command
	pip3 freeze | grep -v "pkg-resources" > requirements.txt

.PHONY: init test
