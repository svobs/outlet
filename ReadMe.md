
# Create a virtual environment within this directory ("matt-project")
following instructions from: https://docs.python.org/3/tutorial/venv.html)
python3 -m venv matt-project
(in a Bash shell):
source matt-project/bin/activate
pip freeze > pip-dependencies.txt

# Install required packages:
pip install -r pip-dependencies.txt
