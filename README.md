
# Create a virtual environment within this directory ("matt_gsuite")
following instructions from: https://docs.python.org/3/tutorial/venv.html)
python3 -m venv matt-project
(in a Bash shell):
source matt_gsuite/bin/activate

# Save required packages
pip freeze > requirements.txt

# Install required packages:
pip install -r requirements.txt

# Install UI prereqs
sudo apt install libgirepository1.0-dev gcc libcairo2-dev pkg-config python3-dev gir1.2-gtk-3.0
pip3 install pycairo
pip install pygobject