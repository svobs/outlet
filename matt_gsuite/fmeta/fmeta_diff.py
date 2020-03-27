
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
        print('Comparing file sets by signature...')
        diff_result = DiffResult()
        for left in set_left.sig_dict.values():
            right_samesig = set_right.sig_dict.get(left.signature, None)
            if right_samesig is None:
                print(f'Left has new file: "{left.file_path}"')
                diff_tree_left.add_item(left, 'New')
                continue

        for right in set_right.sig_dict.values():
            left_samesig = set_left.sig_dict.get(right.signature, None)
            if left_samesig is None:
                print(f'Right has new file: "{right.file_path}"')
                diff_tree_right.add_item(left, 'New')
                continue

        print('Done with diff')
        return diff_result

    @staticmethod
    def diff_by_path(set_left, set_right, diff_tree_left, diff_tree_right):
        print('Comparing file sets by path...')
        diff_result = DiffResult()
        # left represents a unique path
        for left in set_left.path_dict.values():
            right_samepath = set_right.path_dict.get(left.file_path, None)
            if right_samepath is None:
                print(f'Left has new file: "{left.file_path}"')
                # File is added, moved, or copied here.
                # TODO: in the future, be smarter about this
                diff_result.local_adds.append(left)
                continue
            # Do we know this item?
            if right_samepath.signature == left.signature:
                if left.is_valid() and right_samepath.is_valid():
                    # Exact match! Nothing to do.
                    continue
                if left.is_deleted() and right_samepath.is_deleted():
                    # Exact match! Nothing to do.
                    continue
                if left.is_moved() and right_samepath.is_moved():
                    # TODO: figure out where to move to
                    print("DANGER! UNHANDLED 1!")
                    continue

                print(f'DANGER! UNHANDLED 2:{left.file_path}')
                continue
            else:
                print(f'In Left path {left.file_path}: expected signature "{right_samepath.signature}"; actual is "{left.signature}"')
                # Conflict! Need to determine which is most recent
                matching_sig_master = set_right.sig_dict[left.signature]
                if matching_sig_master is None:
                    # This is a new file, from the standpoint of the remote
                    # TODO: in the future, be smarter about this
                    diff_result.local_updates.append(left)
                # print("CONFLICT! UNHANDLED 3!")
                continue

        for right in set_right.path_dict.values():
            left_samepath = set_left.path_dict.get(right.file_path, None)
            if left_samepath is None:
                print(f'Left is missing file: "{right.file_path}"')
                # File is added, moved, or copied here.
                # TODO: in the future, be smarter about this
                diff_result.remote_adds.append(right)
                continue

        print('Done with diff')
        return diff_result

