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

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

# From: https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def is_target_type(filepath, suffixes):
    filepath_lower = filepath.lower()
    for suffix in suffixes:
        regex = '*.' + suffix
        if fnmatch.fnmatch(filepath_lower, regex):
            return True
    return False



def collect_files(path, results):
    pictures_db = open(r"/home/msvoboda/Downloads/pictures.db","w")
    suffixes = [ 'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf' ]
    dest_dir = '/media/msvoboda/SS-USB-200G/GooglePhotos'

    for root, dirs, files in os.walk(path, topdown=True):
        for name in files:
            filepath = os.path.join(root, name)
            if is_target_type(filepath, suffixes):
                # Open,close, read file and calculate MD5 on its contents
                md5_str = md5(filepath)
                # TODO: figure out directory_in_str
                #filepath = re.sub('./', '', filepath, count=1)
                results.append(filepath)
                line = md5_str + ' ' + filepath
                print(line)
                pictures_db.write(line + '\n')
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
    directory_in_str = r"./Svoboda Family Photos"
    path = Path(directory_in_str)

    results = []
    collect_files(path, results)

# this means that if this script is executed, then main() will be executed
if __name__ == '__main__':
    main()
