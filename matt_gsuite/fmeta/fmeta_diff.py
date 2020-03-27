
class DiffResult:
    def __init__(self):
        self.local_adds = []
        self.local_updates = []
        self.local_dels = []
        self.remote_adds = []
        self.remote_dels = []


class FMetaSetDiff:

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

