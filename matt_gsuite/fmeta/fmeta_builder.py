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
from fmeta.fmeta import FMetaSet
from fmeta.fmeta import FMeta
from fmeta.tree_recurser import TreeRecurser
import fmeta.content_hasher
from matt_database import MattDatabase
from pathlib import Path

from widget.progress_meter import ProgressMeter

VALID_SUFFIXES = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'heic', 'mov', 'mp4', 'mpeg', 'mpg', 'm4v', 'avi', 'pdf', 'nef')

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

# SUPPORT CLASSES ####################


class FileCounter(TreeRecurser):
    def __init__(self, root_path):
        TreeRecurser.__init__(self, root_path, valid_suffixes=VALID_SUFFIXES)
        self.files_to_scan = 0

    def handle_target_file_type(self, file_path):
        self.files_to_scan += 1


# Strip the root_path out of the file path:
def strip_root(file_path, root_path):
    root_path_with_slash = root_path if root_path.endswith('/') else root_path + '/'
    return re.sub(root_path_with_slash, '', file_path, count=1)


class FMetaFromFilesBuilder(TreeRecurser):
    def __init__(self, root_path, progress_meter: ProgressMeter, fmeta_set: FMetaSet):
        TreeRecurser.__init__(self, root_path, valid_suffixes=VALID_SUFFIXES)
        self.progress_meter = progress_meter
        self.fmeta_set = fmeta_set

    def build_sync_item(self, file_path):
        # Open,close, read file and calculate MD5 on its contents

        signature_str = fmeta.dropbox_hash(file_path)
        relative_path = strip_root(file_path, str(self.root_path))

        # Get "now" in UNIX time:
        date_time_now = datetime.now()
        sync_ts = int(time.mktime(date_time_now.timetuple()))
        #print(' '.join((str(sync_ts), signature_str, relative_path)))
        length = os.stat(file_path).st_size
        modify_ts = int(os.path.getmtime(file_path))
        return FMeta(signature_str, length, sync_ts, modify_ts, relative_path)

    def handle_target_file_type(self, file_path):
        item = self.build_sync_item(file_path)

        self.fmeta_set.add(item)

        self.progress_meter.add_progress(1)

    def handle_non_target_file(self, file_path):
        print(f'Ignored file: {file_path}')
        item = self.build_sync_item(file_path)
        self.fmeta_set.add_ignored_file(item)


########################################################
# FMetaScanner: build FMetaSet from local tree
class FMetaScanner:
    def __init__(self):
        pass

    @staticmethod
    def scan_local_tree(root_path, progress_meter):
        fmeta_set = FMetaSet(root_path)
        local_path = Path(root_path)
        # First survey our local files:
        print(f'Scanning path: {local_path}')
        file_counter = FileCounter(local_path)
        file_counter.recurse_through_dir_tree()
        print('Found ' + str(file_counter.files_to_scan) + ' files to scan.')
        if progress_meter is not None:
            progress_meter.set_total(file_counter.files_to_scan)
        sync_set_builder = FMetaFromFilesBuilder(local_path, progress_meter, fmeta_set)
        sync_set_builder.recurse_through_dir_tree()
        fmeta_set.print_stats()
        return fmeta_set


#####################################################
# FMetaScanner: build FMetaSet from previously built set in database
class FMetaLoader:
    def __init__(self, db_file_path):
        self.db = MattDatabase(db_file_path)

    def has_data(self):
        return self.db.has_file_changes()

    def build_fmeta_set_from_db(self, root_path):
        fmeta_set = FMetaSet(root_path)

        db_file_changes = self.db.get_file_changes()
        if len(db_file_changes) == 0:
            raise RuntimeError('No data in database!')

        counter = 0
        for change in db_file_changes:
            change.status = 1 # VALID. TODO
            meta = fmeta_set.path_dict.get(change.file_path)
            if meta is None or meta.sync_ts < change.sync_ts:
                fmeta_set.add(change)
                counter += 1

        print(f'Reduced {str(len(db_file_changes))} DB changes into {str(counter)} entries')
        fmeta_set.print_stats()
        return fmeta_set

    def store_fmeta_to_db(self, diff_tree):
        if self.has_data():
            raise RuntimeError('Will not insert FMeta into DB! It is not empty')

        to_insert = diff_tree.fmeta_set.path_dict.values()
        self.db.insert_file_changes(to_insert)
        print(f'Inserted {str(len(to_insert))} FMetas into previously empty DB table.')