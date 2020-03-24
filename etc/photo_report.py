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
from pathlib import Path
import hashlib

class FileEntry:
    def __init__(self, md5, length, file_path):
        self.md5 = md5
        self.length = length
        self.file_path = file_path


if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")


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


by_md5 = {}
by_path = {}

# Algorithm:
# 1. Iterate over directory tree and build metadata for ENTIRE tree: first do file paths, then loop around and do MD5s & file length
# 1a. Need to look up by file path, and also by MD5 (2 structures)
# 3. Is file 0 bytes? -> add to list of "bad" items
# 3. Look up MD5 in DB. Look up filepath in DB
# 3a. Found MD5 in different location? -> update entry with new path in DB,
# 3b. Nothing found with that MD5? -> create new entry in DB, add to list of "new" items
def collect_files(path):
    pictures_db = open(r"/home/msvoboda/Downloads/pictures.db","w")
    suffixes = ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf']
    dest_dir = '/media/msvoboda/SS-USB-200G/GooglePhotos'

    for root, dirs, files in os.walk(path, topdown=True):
        for name in files:
            filepath = os.path.join(root, name)
            if is_target_type(filepath, suffixes):
                # Open,close, read file and calculate MD5 on its contents
                md5_str = md5(filepath)
                # TODO: figure out directory_in_str
                #filepath = re.sub('./', '', filepath, count=1)

                line = md5_str + ' ' + filepath
                print(line)
                pictures_db.write(line + '\n')

                length = os.stat(filepath).st_size
                entry = FileEntry(md5, length, filepath)
                by_md5[md5_str] = entry
                by_path[filepath] = entry
#                dest_path = os.path.join(dest_dir, name)
#                shutil.copyfile(filepath, dest_path)
            else:
                line = '### ' + filepath
                print(line)
                pictures_db.write(line + '\n')

        #for name in dirs:
            #print('DIR:' + os.path.join(root, name))

    pictures_db.close()


def main():
    directory_in_str = r"/home/msvoboda/GoogleDrive/Media/Svoboda-Family/Svoboda Family Photos"
    path = Path(directory_in_str)

    collect_files(path)
    print("By_MD5 count: " + str(len(by_md5)))
    print("By_Path count: " + str(len(by_path)))


# this means that if this script is executed, then main() will be executed
if __name__ == '__main__':
    main()
