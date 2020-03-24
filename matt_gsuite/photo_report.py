#!/usr/bin/python3

# exiftool metadata:
# exiftool -AllDates="2001:01:01 12:00:00" *
# exiftool -Comment="Hawaii" ./2001\ Hawaii\ *
# find . -name "*jpg_original" -exec rm -fv {} \;

# Compare the output of this script with `tree -a .`
import sys
import fnmatch
import re
import os
import shutil
from datetime import datetime
import time
from file_meta import FilesMeta
from file_meta import FileEntry
from matt_database import MattDatabase
from pathlib import Path
import hashlib


if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

# TODO: switch to SHA-2
# From: https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def md5(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def is_target_type(file_path, suffixes):
    file_path_lower = file_path.lower()
    for suffix in suffixes:
        regex = '*.' + suffix
        if fnmatch.fnmatch(file_path_lower, regex):
            return True
    return False


local_files_meta = FilesMeta()

# Algorithm:
# 1. Iterate over directory tree and build metadata for ENTIRE tree: first do file paths, then loop around and do MD5s & file length
# 1a. Need to look up by file path, and also by MD5 (2 structures)
# 3. Is file 0 bytes? -> add to list of "bad" items
# 3. Look up MD5 in DB. Look up filepath in DB
# 3a. Found MD5 in different location? -> update entry with new path in DB,
# 3b. Nothing found with that MD5? -> create new entry in DB, add to list of "new" items
def collect_files(root_path, target_file_handler_func, non_target_handler_func):
    suffixes = ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf']
    dest_dir = '/media/msvoboda/SS-USB-200G/GooglePhotos'

    for root, dirs, files in os.walk(root_path, topdown=True):
        for name in files:
            file_path = os.path.join(root, name)
            if is_target_type(file_path, suffixes):
                target_file_handler_func(file_path, root_path)
            else:
                non_target_handler_func(file_path, root_path)

        #for name in dirs:
            #print('DIR:' + os.path.join(root, name))


# Strip the root_path out of the file path:
def strip_root(file_path, root_path):
    root_path_with_slash = root_path if root_path.endswith('/') else root_path + '/'
    return re.sub(root_path_with_slash, '', file_path, count=1)


def build_meta_for_file(file_path, root_path):

    # Open,close, read file and calculate MD5 on its contents
    signature_str = md5(file_path)
    relative_path = strip_root(file_path, str(root_path))

    line = signature_str + ' ' + relative_path
    print(line)

    # Get "now" in UNIX time:
    date_time_now = datetime.now()
    sync_ts = int(time.mktime(date_time_now.timetuple()))
    print(sync_ts)

    length = os.stat(file_path).st_size
    entry = FileEntry(signature_str, length, sync_ts, relative_path)
    local_files_meta.sig_dict[signature_str] = entry
    local_files_meta.path_dict[relative_path] = entry


def handle_unexpected_file(file_path, root_path):
    line = '### UNEXPECTED FILE: ' + file_path
    print(line)


def main():
    directory_in_str = r"/home/msvoboda/GoogleDrive/Media/Svoboda-Family/Svoboda Family Photos"
    path = Path(directory_in_str)

    # First, build meta structures:
    collect_files(path, build_meta_for_file, handle_unexpected_file)
    print("By_MD5 count: " + str(len(local_files_meta.md5_dict)))
    print("By_Path count: " + str(len(local_files_meta.path_dict)))

    db_files_meta = FilesMeta()

    # Anything in the DB? If not, store in DB.
    db = MattDatabase('MattGSuite.db')
    file_changes = db.get_file_changes()
    #for file_change in file_changes:
        # TODO build path struct
        # TODO only keep latest change for each path


    # Else compare with what is in the DB


# this means that if this script is executed, then main() will be executed
if __name__ == '__main__':
    main()
