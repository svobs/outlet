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
from datetime import datetime
import time
from file_meta import FilesMeta
from file_meta import FileEntry
from file_meta import SyncSet
from matt_database import MattDatabase
from pathlib import Path
import hashlib

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef')

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")


class DirDiffer:
    def __init__(self, db_file_path):
        self.db = MattDatabase(db_file_path)
        self.files_to_scan = 0
        self.progress_meter = None

    def diff_full(self, local_path_string, progress_meter):
        local_path = Path(local_path_string)
        # First survey our local files:
        print(f'Scanning path: {local_path}')
        self.files_to_scan = 0
        self.progress_meter = progress_meter
        scan_directory_tree(Path(local_path), self.count_files, None)
        print('Found ' + str(self.files_to_scan) + ' files to scan.')
        if self.progress_meter is not None:
            self.progress_meter.set_total(self.files_to_scan)
        local_files_meta = scan_directory_tree(local_path, self.build_meta_for_file, self.handle_unexpected_file)
        print("Sig count: " + str(len(local_files_meta.sig_dict)))
        print("Path count: " + str(len(local_files_meta.path_dict)))

        # Anything in the DB? If not, store in DB.
        db_file_changes = self.db.get_file_changes()
        if len(db_file_changes) == 0:
            to_insert = local_files_meta.path_dict.values()
            self.db.insert_file_changes(to_insert)
            print(f'Inserted {str(len(to_insert))} file paths into previously empty DB table.')
            return

        # Else compare with what is in the DB
        db_files_meta = build_files_meta_from_db(db_file_changes)
        sync_set = compare(db_files_meta, local_files_meta)
        return sync_set

    def count_files(self, file_path, root_path, local_files_meta):
        self.files_to_scan += 1

    def build_meta_for_file(self, file_path, root_path, local_files_meta):

        # Open,close, read file and calculate MD5 on its contents
        signature_str = md5(file_path)
        relative_path = strip_root(file_path, str(root_path))

        # Get "now" in UNIX time:
        date_time_now = datetime.now()
        sync_ts = int(time.mktime(date_time_now.timetuple()))

        #print(' '.join((str(sync_ts), signature_str, relative_path)))

        length = os.stat(file_path).st_size
        entry = FileEntry(signature_str, length, sync_ts, relative_path)
        local_files_meta.sig_dict[signature_str] = entry
        local_files_meta.path_dict[relative_path] = entry

        self.progress_meter.add_progress(1)

    # TODO: add unexpected file to list
    def handle_unexpected_file(self, file_path, root_path, local_files_meta):
        line = '### UNEXPECTED FILE: ' + file_path
        print(line)



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


# Algorithm:
# 1. Iterate over directory tree and build metadata for ENTIRE tree: first do file paths, then loop around and do MD5s & file length
# 1a. Need to look up by file path, and also by MD5 (2 structures)
# 3. Is file 0 bytes? -> add to list of "bad" items
# 3. Look up MD5 in DB. Look up filepath in DB
# 3a. Found MD5 in different location? -> update entry with new path in DB,
# 3b. Nothing found with that MD5? -> create new entry in DB, add to list of "new" items
def scan_directory_tree(root_path, target_file_handler_func, non_target_handler_func):
    local_files_meta = FilesMeta()

    for root, dirs, files in os.walk(root_path, topdown=True):
        for name in files:
            file_path = os.path.join(root, name)
            if is_target_type(file_path, VALID_SUFFIXES):
                if target_file_handler_func is not None:
                    target_file_handler_func(file_path, root_path, local_files_meta)
            else:
                if non_target_handler_func is not None:
                    non_target_handler_func(file_path, root_path, local_files_meta)

        #for name in dirs:
            #print('DIR:' + os.path.join(root, name))

    return local_files_meta


# Strip the root_path out of the file path:
def strip_root(file_path, root_path):
    root_path_with_slash = root_path if root_path.endswith('/') else root_path + '/'
    return re.sub(root_path_with_slash, '', file_path, count=1)


# Param 'db_file_changes' is a list of FileEntry objects
def build_files_meta_from_db(db_file_changes):
    db_files_meta = FilesMeta()

    counter = 0
    for change in db_file_changes:
        change.deleted = 1 # VALID. TODO
        meta = db_files_meta.path_dict.get(change.file_path)
        if meta is None or meta.sync_ts < change.sync_ts:
            db_files_meta.sig_dict[change.signature] = change
            db_files_meta.path_dict[change.file_path] = change
            counter = counter + 1

    print(f'Reduced {str(len(db_file_changes))} changes into {str(counter)} entries')
    return db_files_meta


def compare(set_meta_master, set_meta_local):
    print('Comparing local file set against most recent sync...')
    sync_set = SyncSet()
    # meta_local represents a unique path
    for meta_local in set_meta_local.path_dict.values():
        matching_path_master = set_meta_master.path_dict.get(meta_local.file_path, None)
        if matching_path_master is None:
            print(f'Local has new file: "{meta_local.file_path}"')
            # File is added, moved, or copied here.
            # TODO: in the future, be smarter about this
            sync_set.local_adds.append(meta_local)
            continue
        # Do we know this item?
        if matching_path_master.signature == meta_local.signature:
            if meta_local.is_valid() and matching_path_master.is_valid():
                # Exact match! Nothing to do.
                continue
            if meta_local.is_deleted() and matching_path_master.is_deleted():
                # Exact match! Nothing to do.
                continue
            if meta_local.is_moved() and matching_path_master.is_moved():
                # TODO: figure out where to move to
                print("DANGER! UNHANDLED 1!")
                continue

            print(f'DANGER! UNHANDLED 2:{meta_local.file_path}')
            continue
        else:
            print(f'In path {meta_local.file_path}: expected signature "{matching_path_master.signature}"; actual is "{meta_local.signature}"')
            # Conflict! Need to determine which is most recent
            matching_sig_master = set_meta_master.sig_dict[meta_local.signature]
            if matching_sig_master is None:
                # This is a new file, from the standpoint of the remote
                # TODO: in the future, be smarter about this
                sync_set.local_updates.append(meta_local)
               # print("CONFLICT! UNHANDLED 3!")
            continue

    for meta_master in set_meta_master.path_dict.values():
        matching_path_local = set_meta_local.path_dict.get(meta_master.file_path, None)
        if matching_path_local is None:
            print(f'Local is missing file: "{meta_master.file_path}"')
            # File is added, moved, or copied here.
            # TODO: in the future, be smarter about this
            sync_set.remote_adds.append(meta_master)
            continue

    return sync_set
