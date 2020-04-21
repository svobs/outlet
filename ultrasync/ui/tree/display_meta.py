class TreeDisplayMeta:
    def __init__(self, config, editable, is_ignored_func=None):
        self.config = config
        self.editable = editable
        # This is a function pointer which accepts a data node arg and returns true if it is considered "ignored":
        self.is_ignored_func = is_ignored_func

        """If true, create a node for each ancestor directory for the files.
           If false, create a second column which shows the parent path. """
        self.use_dir_tree = config.get('display.diff_tree.use_dir_tree')
        self.show_change_ts = config.get('display.diff_tree.show_change_ts')
        self.datetime_format = config.get('display.diff_tree.datetime_format')
        self.extra_indent = config.get('display.diff_tree.extra_indent')
        self.row_height = config.get('display.diff_tree.row_height')

        col_count = 0
        self.col_types = []
        self.col_names = []
        if self.editable:
            self.col_num_checked = col_count
            self.col_names.append('Checked')
            self.col_types.append(bool)
            col_count += 1

            self.col_num_inconsistent = col_count
            self.col_names.append('Inconsistent')
            self.col_types.append(bool)
            col_count += 1
        self.col_num_icon = col_count
        self.col_names.append('Icon')
        self.col_types.append(str)
        col_count += 1

        self.col_num_name = col_count
        self.col_names.append('Name')
        self.col_types.append(str)
        col_count += 1

        if not self.use_dir_tree:
            self.col_num_directory = col_count
            self.col_names.append('Directory')
            self.col_types.append(str)
            col_count += 1

        self.col_num_size = col_count
        self.col_names.append('Size')
        self.col_types.append(str)
        col_count += 1

        self.col_num_modification_ts = col_count
        self.col_names.append('Modification Time')
        self.col_types.append(str)
        col_count += 1

        if self.show_change_ts:
            self.col_num_change_ts = col_count
            self.col_names.append('Meta Change Time')
            self.col_types.append(str)
            col_count += 1

        self.col_num_data = col_count
        self.col_names.append('Data')
        self.col_types.append(object)
        col_count += 1

