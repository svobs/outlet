from model.category import Category


def build_command_list(tree):
    for node in tree.get_all():
        if node.category == Category.Added:
            pass
        elif node.category == Category.Moved:
            pass
        elif node.category == Category.Deleted:
            pass
        elif node.category == Category.Updated:
            pass
        # TODO
