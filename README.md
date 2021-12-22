
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

# PROJECT GOALS

1. Robustness. As much as possible, the app should be killable at any point and suffer no data loss, or even loss of UI state.
2. Responsiveness. The user should never have to wait (except during indexing, and even then that should be minimized as much as possible). Actions should return as quickly as possible, and should be allowed to be compounded into addtional actions. Long-running operations from the user should be queued appropriately.
2. Declarative behavior. The user should never have any reason to doubt that its actions are going to be fulfilled. The transition to a future state desired by the user should be represented by the UI, it should be persisted every step along the way, and a record of it should be kept which is so good that we can eventually undo. Any errors which prevent a state change from happening should be clear.


## Future Major Features
### Various small items
* Checkbox: Prioritize reading over writing
    * Scan the content of flles before overwriting, and do not overwrite if the content is already there
* TODO: Add ability to cancel a UserOp and all pending ops
* TODO (minor): blank out selection & expanded states when changing tree root
* TODO (minor): only scan dirs inside display tree when loading
* TODO (minor): handle case where root path is deleted
* TODO: allow rules to be created for different directories.
    * e.g.: Copy to: X Directory on Device A
    * e.g.: Move to: Y Directory on Device B
    * For 2-way sync: just create 2 copy rules with each directory pointing at the other
* TODO: Add ability to cancel pending task from UI

### Future feature idea: put old content in trash & only delete when needed
    The command cannot be completed because X Disk needs an additional 52 MB of free space, unless you first empty at least 24% of its trash.

    * Delete Oldest 24%
    * Delete All Trash

### Audit UIDs:
1. Make one giant map of UID -> thing 
2. Iterate over all nodes

#### ESSENTIAL FOR FIRST RELEASE
* [3] Google Drive connect flow
* [5] Mac installer
* [3] Google Drive single-parent migration check & assistant (see: https://developers.google.cn/drive/api/v3/multi-parenting?hl=zh-cn)
* [1] Better tracking of BE readiness states: see CentralExecutor.get_engine_summary_state()
* [5] Mode Toolbar with Cut, Copy modes; Merge Folder Toolbar with Add if Not Present, Overwrite Conflicts vs Ignore Conflicts, Delete Extraneous vs Keep Extraneous toggles
  
#### FUTURE / NON-ESSENTIAL
* [3] Support for extra GDrive types (e.g. shortcuts, Google Docs files)
* [3] Cascade failures and allow recovery
* [1] Copy metadata when copying files (MAC times)
* [1] Add modification dates for local dirs
* [3] Extend config file to support windows on different servers
* [1] Put GDrive files in trash
* [3] Put local files in trash
* [3] Tombstone trash support
* [5] Run multiple concurrent ops
* [3] Optimal Path - CommandBuilder: look up MD5 for src_node and use a closer node
* [1] Content UID <-> MD5/SHA256 mappings
* [5] OpManager: simplify the op tree each time the next change requested
* [3] Bulk delete dir trees instead of one by one
* [5] Progress bar for current task + view all pending & current tasks
