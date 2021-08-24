
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

#### ESSENTIAL FOR FIRST RELEASE
* [3] BFS loading of tree, instead of all-at-once. Calc MD5s only after tree loaded [IN PROGRESS]

* [3] Google Drive connect flow
* [3] Mac installer
* [1] Google Drive single-parent migration check & assistant (see: https://developers.google.cn/drive/api/v3/multi-parenting?hl=zh-cn)
* [1] Better tracking of BE readiness states: see CentralExecutor.get_engine_summary_state()
* [5] Mode Toolbar with Cut, Copy modes; Merge Folder Toolbar with Add if Not Present, Overwrite Conflicts vs Ignore Conflicts, Delete Extraneous vs Keep Extraneous toggles
* [3] Disable UI elements for trees until cache loaded
  
#### FUTURE / NON-ESSENTIAL
* [1] Copy metadata when copying files (MAC times)
* [1] Add modification dates for local dirs
* [3] Extend config file to support windows on different servers
* [1] Put GDrive files in trash
* [3] Put local files in trash
* [3] Tombstone trash support
* [5] Run multiple concurrent ops
* [3] Optimal Path - CommandBuilder: look up MD5 for src_node and use a closer node
* [1] UID <-> MD5/SHA256 mappings
* [5] OpManager: simplify the op tree each time the next change requested
* [3] Bulk delete dir trees instead of one by one
* [5] Progress bar for current task + view all pending & current tasks
