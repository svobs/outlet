#!/usr/bin/python3

# exiftool metadata:
# exiftool -AllDates="2001:01:01 12:00:00" *
# exiftool -Comment="Hawaii" ./2001\ Hawaii\ *
# find . -name "*jpg_original" -exec rm -fv {} \;

# Compare the output of this script with `tree -a .`
import sys
import re
import os
from datetime import datetime
import time
from sync_item import SyncSet
from sync_item import SyncItem
from sync_item import DiffResult
from tree_recurser import TreeRecurser
from matt_database import MattDatabase
from pathlib import Path
import hashlib

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef')

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")


class FileCounter(TreeRecurser):
    def __init__(self, root_path):
        TreeRecurser.__init__(self, root_path, valid_suffixes=VALID_SUFFIXES)
        self.files_to_scan = 0

    def handle_target_file_type(self, file_path):
        self.files_to_scan += 1


# TODO: switch to SHA-2
# From: https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def md5(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# Strip the root_path out of the file path:
def strip_root(file_path, root_path):
    root_path_with_slash = root_path if root_path.endswith('/') else root_path + '/'
    return re.sub(root_path_with_slash, '', file_path, count=1)


class SyncSetBuilder(TreeRecurser):
    def __init__(self, root_path, progress_meter, diff_tree):
        TreeRecurser.__init__(self, root_path, valid_suffixes=VALID_SUFFIXES)
        self.progress_meter = progress_meter
        self.diff_tree = diff_tree

    def build_sync_item(self, file_path):
        # Open,close, read file and calculate MD5 on its contents
        signature_str = md5(file_path)
        relative_path = strip_root(file_path, str(self.root_path))

        # Get "now" in UNIX time:
        date_time_now = datetime.now()
        sync_ts = int(time.mktime(date_time_now.timetuple()))
        #print(' '.join((str(sync_ts), signature_str, relative_path)))
        length = os.stat(file_path).st_size
        return SyncItem(signature_str, length, sync_ts, relative_path)

    def handle_target_file_type(self, file_path):
        item = self.build_sync_item(file_path)

        self.sync_set.sig_dict[item.signature] = item
        self.sync_set.path_dict[item.signature] = item

        self.progress_meter.add_progress(1)

    # TODO: add unexpected file to list
    def handle_non_target_file(self, file_path):
        print('### UNEXPECTED FILE: ' + file_path)
        item = self.build_sync_item(file_path)
        self.diff_tree.add_unexpected_item(item)


class DirDiffer:
    def __init__(self, db_file_path):
        self.db = MattDatabase(db_file_path)

    @staticmethod
    def scan_local_tree(local_path_string, progress_meter, diff_tree):
        local_path = Path(local_path_string)
        # First survey our local files:
        print(f'Scanning path: {local_path}')
        file_counter = FileCounter(local_path)
        file_counter.recurse_through_dir_tree()
        print('Found ' + str(file_counter.files_to_scan) + ' files to scan.')
        if progress_meter is not None:
            progress_meter.set_total(file_counter.files_to_scan)
        sync_set_builder = SyncSetBuilder(local_path, progress_meter, diff_tree)
        sync_set_builder.recurse_through_dir_tree()
        print("Sig count: " + str(len(sync_set_builder.sync_set.sig_dict)))
        print("Path count: " + str(len(sync_set_builder.sync_set.path_dict)))
        return sync_set_builder.sync_set

    @staticmethod
    def diff(set_left, set_right, diff_tree_left, diff_tree_right):
        print('Comparing local file set against most recent sync...')
        diff_result = DiffResult()
        # meta_local represents a unique path
        for meta_local in set_left.path_dict.values():
            matching_path_master = set_right.path_dict.get(meta_local.file_path, None)
            if matching_path_master is None:
                print(f'Local has new file: "{meta_local.file_path}"')
                # File is added, moved, or copied here.
                # TODO: in the future, be smarter about this
                diff_result.local_adds.append(meta_local)
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
                matching_sig_master = set_right.sig_dict[meta_local.signature]
                if matching_sig_master is None:
                    # This is a new file, from the standpoint of the remote
                    # TODO: in the future, be smarter about this
                    diff_result.local_updates.append(meta_local)
                # print("CONFLICT! UNHANDLED 3!")
                continue

        for meta_master in set_right.path_dict.values():
            matching_path_local = set_left.path_dict.get(meta_master.file_path, None)
            if matching_path_local is None:
                print(f'Local is missing file: "{meta_master.file_path}"')
                # File is added, moved, or copied here.
                # TODO: in the future, be smarter about this
                diff_result.remote_adds.append(meta_master)
                continue

        return diff_result


    # Algorithm:
    # 1. Iterate over directory tree and build metadata for ENTIRE tree: first do file paths, then loop around and do MD5s & file length
    # 1a. Need to look up by file path, and also by MD5 (2 structures)
    # 3. Is file 0 bytes? -> add to list of "bad" items
    # 3. Look up MD5 in DB. Look up filepath in DB
    # 3a. Found MD5 in different location? -> update entry with new path in DB,
    # 3b. Nothing found with that MD5? -> create new entry in DB, add to list of "new" items
    #def diff_full(self, sync_set_left, sync_set_right, progress_meter, diff_tree_left, diff_tree_right):

        # Anything in the DB? If not, store in DB.
        # db_file_changes = self.db.get_file_changes()
        # if len(db_file_changes) == 0:
        #     to_insert = sync_set.path_dict.values()
        #     self.db.insert_file_changes(to_insert)
        #     print(f'Inserted {str(len(to_insert))} file paths into previously empty DB table.')
        #     return
        # Else compare with what is in the DB
        # db_files_meta = build_files_meta_from_db(db_file_changes)








# Param 'db_file_changes' is a list of SyncItem objects
def build_files_meta_from_db(db_file_changes):
    db_files_meta = SyncSet()

    counter = 0
    for change in db_file_changes:
        change.status = 1 # VALID. TODO
        meta = db_files_meta.path_dict.get(change.file_path)
        if meta is None or meta.sync_ts < change.sync_ts:
            db_files_meta.sig_dict[change.signature] = change
            db_files_meta.path_dict[change.file_path] = change
            counter = counter + 1

    print(f'Reduced {str(len(db_file_changes))} changes into {str(counter)} entries')
    return db_files_meta

