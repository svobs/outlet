
class FMetaSetDiff:

    @staticmethod
    def diff(left_tree, right_tree):
        print('Comparing file sets by signature...')
        for left_metas in left_tree.fmeta_set.sig_dict.values():
            left_meta = left_metas[0]
            right_samesig = right_tree.fmeta_set.sig_dict.get(left_meta.signature, None)
            if right_samesig is None:
                #print(f'Left has new file: "{left.file_path}"')
                left_tree.change_set.adds.append(left_meta)
                continue

        for right_metas in right_tree.fmeta_set.sig_dict.values():
            right_meta = right_metas[0]
            left_samesig = left_tree.fmeta_set.sig_dict.get(right_meta.signature, None)
            if left_samesig is None:
                #print(f'Right has new file: "{right.file_path}"')
                right_tree.change_set.adds.append(right_meta)
                continue

        print(f'Done with diff. LeftAdds={len(left_tree.change_set.adds)} RightAdds={len(right_tree.change_set.adds)}')

    @staticmethod
    def diff_by_path(left_tree, right_tree):
        print('Comparing file sets by path...')
        # left represents a unique path
        for left in left_tree.fmeta_set.path_dict.values():
            right_samepath = right_tree.fmeta_set.path_dict.get(left.file_path, None)
            if right_samepath is None:
                print(f'Left has new file: "{left.file_path}"')
                # File is added, moved, or copied here.
                # TODO: in the future, be smarter about this
                left.change_set.adds.append(left)
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
                matching_sig_master = right_tree.fmeta_set.sig_dict[left.signature]
                if matching_sig_master is None:
                    # This is a new file, from the standpoint of the remote
                    # TODO: in the future, be smarter about this
                    left_tree.change_set.updates.append(left)
                # print("CONFLICT! UNHANDLED 3!")
                continue

        for right in right_tree.fmeta_set.path_dict.values():
            left_samepath = left_tree.fmeta_set.path_dict.get(right.file_path, None)
            if left_samepath is None:
                print(f'Left is missing file: "{right.file_path}"')
                # File is added, moved, or copied here.
                # TODO: in the future, be smarter about this
                right_tree.change_set.adds.append(right)
                continue

        print('Done with diff')

