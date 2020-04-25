import logging

from cache.base_db import MetaDatabase

logger = logging.getLogger(__name__)


class GDriveDatabase(MetaDatabase):
    TABLE_GRDIVE_DIRS = {
        'name': 'gdrive_directory',
        'cols': (('id', 'TEXT'),
                 ('name', 'TEXT'),
                 ('parent_id', 'TEXT'),
                 ('trashed', 'INTEGER'),
                 ('drive_id', 'TEXT'),
                 ('my_share', 'INTEGER'),
                 ('sync_ts', 'INTEGER'))
        # TODO: children_fetched
    }

    TABLE_GRDIVE_MULTIPLE_PARENTS = {
        'name': 'gdrive_multiple_parents',
        'cols': (('id', 'TEXT'),)
    }

    TABLE_GRDIVE_FILES = {
        'name': 'gdrive_files',
        'cols': (('id', 'TEXT'),
                 ('name', 'TEXT'),
                 ('parent_id', 'TEXT'),
                 ('trashed', 'INTEGER'),
                 ('size_bytes', 'INTEGER'),
                 ('md5', 'TEXT'),
                 ('create_ts', 'INTEGER'),
                 ('modify_ts', 'INTEGER'),
                 ('owner_id', 'TEXT'),
                 ('drive_id', 'TEXT'),
                 ('my_share', 'INTEGER'),
                 ('version', 'INTEGER'),
                 ('head_revision_id', 'TEXT'),
                 ('sync_ts', 'INTEGER'))

    }

    def __init__(self, db_path):
        super().__init__(db_path)

    # GDRIVE_DIRS operations ---------------------

    def truncate_gdrive_dirs(self):
        self.truncate_table(self.TABLE_GRDIVE_MULTIPLE_PARENTS)
        self.truncate_table(self.TABLE_GRDIVE_DIRS)

    def has_gdrive_dirs(self):
        return self.has_rows(self.TABLE_GRDIVE_DIRS) or self.has_rows(self.TABLE_GRDIVE_MULTIPLE_PARENTS)

    def insert_gdrive_dirs(self, dir_list, overwrite=False):
        if self.has_gdrive_dirs():
            if overwrite:
                self.drop_table_if_exists(self.TABLE_GRDIVE_DIRS)
                self.create_table_if_not_exist(self.TABLE_GRDIVE_DIRS)
            else:
                raise RuntimeError('Will not insert GDrive meta into a non-empty table!')

        self.insert_many(self.TABLE_GRDIVE_DIRS, dir_list)

    def get_gdrive_dirs(self):
        dir_rows = self.get_all_rows(self.TABLE_GRDIVE_DIRS)

        logger.debug(f'Retrieved {len(dir_rows)} dirs')
        return dir_rows

    # GDRIVE_FILES operations ---------------------

    def has_gdrive_files(self):
        return self.has_rows(self.TABLE_GRDIVE_FILES)

    def insert_gdrive_files(self, file_list, overwrite=False):
        if self.has_gdrive_files():
            if overwrite:
                self.drop_table_if_exists(self.TABLE_GRDIVE_FILES)
                self.create_table_if_not_exist(self.TABLE_GRDIVE_FILES)
            else:
                raise RuntimeError('Cannot insert GDrive file meta into a non-empty table (Overwrite=False)')

        self.insert_many(self.TABLE_GRDIVE_FILES, file_list)

    def get_gdrive_files(self):
        file_rows = self.get_all_rows(self.TABLE_GRDIVE_FILES)

        logger.debug(f'Retrieved {len(file_rows)} file metas')
        return file_rows

    def insert_multiple_parent_mappings(self, ids_with_multiple_parents, overwrite=False):
        if self.is_table(self.TABLE_GRDIVE_MULTIPLE_PARENTS):
            if overwrite:
                self.drop_table_if_exists(self.TABLE_GRDIVE_MULTIPLE_PARENTS)
                self.create_table_if_not_exist(self.TABLE_GRDIVE_MULTIPLE_PARENTS)
            else:
                raise RuntimeError('Cannot insert GDrive parent meta into a non-empty table (overwrite=False)')

        self.insert_many(self.TABLE_GRDIVE_MULTIPLE_PARENTS, ids_with_multiple_parents)

    def get_multiple_parent_ids(self):
        """this will include extraneous stuff from parent meta"""
        parent_ids = self.get_all_rows(self.TABLE_GRDIVE_MULTIPLE_PARENTS)
        logger.debug(f'Retrieved {len(parent_ids)} items with multiple parents')
        return parent_ids
