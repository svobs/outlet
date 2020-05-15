
# Create a virtual environment within this directory ({project_name})
following instructions from: https://docs.python.org/3/tutorial/venv.html)
cd ..
python3 -m venv {projectName}
# (in a Bash shell):
source {projectName}/bin/activate

# Save required packages
make freeze

# Install required packages:
make init

# Install UI prereqs
sudo apt install libgirepository1.0-dev gcc libcairo2-dev pkg-config python3-dev gir1.2-gtk-3.0
pip3 install pycairo
pip install pygobject

#### Misc EXIF Tool Notes
exiftool -AllDates="2001:01:01 12:00:00" *

exiftool -Comment="Hawaii" {target_dir}
find . -name "*jpg_original" -exec rm -fv {} \;
