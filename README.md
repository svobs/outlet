# Setup

## Mac
* For UTF-8 display: need to install the Symbola font found here:
https://fontlibrary.org/assets/downloads/symbola/cf81aeb303c13ce765877d31571dc5c7/symbola.zip

## Ubuntu
    sudo apt-get install libarchive-dev  
    sudo apt-get install libyaml-dev

Create a virtual environment within this directory (`{project_name}`)  following instructions from: https://docs.python.org/3/tutorial/venv.html)  

    cd ..
    python3 -m venv {projectName}
    # In a bash shell:
    source {projectName}/bin/activate

#### Install GTK3 UI prereqs
    sudo apt install libgirepository1.0-dev gcc libcairo2-dev pkg-config python3-dev gir1.2-gtk-3.0
    pip3 install pycairo
    pip3 install pygobject

#### (Optional) Misc EXIF Tool Notes
    exiftool -AllDates="2001:01:01 12:00:00" *
    exiftool -Comment="Hawaii" {target_dir}
    find . -name "*jpg_original" -exec rm -fv {} \;

#### Save required packages
    make freeze

#### Install required packages:
    make init

### Future Major Features (TODO)
* Tombstones
* Reload GDrive
* Pause/resume ops
* Detach service
* Run multiple concurrent ops
* CommandBuilder: look up MD5 for src_node and use a closer node
* OpLedger: simplify the op tree each time the next change is requested
* Filter UI by Trashed, Shared, etc.
* Filter by name (aka search)
* Put GDrive files in trash
* Put local files in trash
* Bulk delete dir trees instead of one by one
