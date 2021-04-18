# Setup

## Mac
### For UTF-8 display
Need to install the Symbola font found here:
https://fontlibrary.org/assets/downloads/symbola/cf81aeb303c13ce765877d31571dc5c7/symbola.zip
### Manually install zlib for Pillow
    brew install zlib
    export PKG_CONFIG_PATH="/usr/local/opt/zlib/lib/pkgconfig"
    python3 -m pip install Pillow==7.1.0

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
* [3] Rewrite all SQLite stuff with SQL-injection safe code
* [3] Disable UI elements for trees until cache loaded
* [3] Device selection for root path
* [5] BFS loading of tree, instead of all-at-once
* [3] Stats updating - needs cleanup
* [7] Mac OS support
* [1] Put GDrive files in trash
* [3] Put local files in trash
* [3] Tombstone trash support
* [3] Tombstones
* [5] Run multiple concurrent ops
* [3] Optimal Path - CommandBuilder: look up MD5 for src_node and use a closer node
* [1] UID <-> MD5/SHA256
* [5] OpLedger: simplify the op tree each time the next change is requested
* Bulk delete dir trees instead of one by one
