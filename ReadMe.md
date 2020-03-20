
# Create a virtual environment within this directory ("matt-project")
following instructions from: https://docs.python.org/3/tutorial/venv.html)
python3 -m venv matt-project
(in a Bash shell):
source matt-project/bin/activate

# Save required packages
pip freeze > requirements.txt

# Install required packages:
pip install -r requirements.txt
