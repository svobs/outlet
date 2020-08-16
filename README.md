
## In Ubuntu (TODO: is this required?)
sudo apt-get install libarchive-dev

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

# (Mac only): for UTF-8 display
Need to install the Symbola font found here:
https://fontlibrary.org/assets/downloads/symbola/cf81aeb303c13ce765877d31571dc5c7/symbola.zip

# Install UI prereqs
sudo apt install libgirepository1.0-dev gcc libcairo2-dev pkg-config python3-dev gir1.2-gtk-3.0
pip3 install pycairo
pip install pygobject

#### Misc EXIF Tool Notes
exiftool -AllDates="2001:01:01 12:00:00" *

exiftool -Comment="Hawaii" {target_dir}
find . -name "*jpg_original" -exec rm -fv {} \;

### Future Features (TODO)
* CommandBuilder: look up MD5 for src_node and use a closer node
* ChangeLedger: simplify the op tree each time the next change is requested
