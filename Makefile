init:
	#cd ..
	#python3 -m venv ultrasync
	#cd ultrasync
	#./bin/activate
	pip3 install -r requirements.txt

test:
	py.test tests

clean:
	rm -rf venv include lib lib64

freeze:
	# See https://stackoverflow.com/questions/39577984/what-is-pkg-resources-0-0-0-in-output-of-pip-freeze-command
	pip3 freeze | grep -v "pkg-resources" > requirements.txt

.PHONY: init test
