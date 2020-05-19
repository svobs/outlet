from command.command import CommandList, CopyFileLocallyCommand, DeleteGDriveFileCommand, DeleteLocalFileCommand, DownloadFromGDriveCommand, \
    MoveFileGDriveCommand, \
    MoveFileLocallyCommand, \
    UploadToGDriveCommand
from constants import OBJ_TYPE_GDRIVE, OBJ_TYPE_LOCAL_DISK
from model.category import Category
from model.planning_node import FileToAdd, FileToMove, FileToUpdate


class CommandBuilder:
    def __init__(self, uid_generator):
        self._uid_generator = uid_generator

    def build_command_list(self, tree):
        cmd_list: CommandList = CommandList(self._uid_generator.get_new_uid())

        for node in tree.get_all():
            tree_type: int = node.identifier.tree_type
            if node.category == Category.Added:
                assert isinstance(node, FileToAdd)
                orig_tree_type: int = node.src_node.identifier.tree_type
                if orig_tree_type == tree_type:
                    if tree_type == OBJ_TYPE_LOCAL_DISK:
                        cmd = CopyFileLocallyCommand(model_obj=node)
                    elif tree_type == OBJ_TYPE_GDRIVE:
                        raise RuntimeError(f'Bad tree type: {tree_type}')
                    else:
                        raise RuntimeError(f'Bad tree type: {tree_type}')
                elif orig_tree_type == OBJ_TYPE_LOCAL_DISK and tree_type == OBJ_TYPE_GDRIVE:
                    cmd = UploadToGDriveCommand(model_obj=node)
                elif orig_tree_type == OBJ_TYPE_GDRIVE and tree_type == OBJ_TYPE_LOCAL_DISK:
                    cmd = DownloadFromGDriveCommand(model_obj=node)
                else:
                    raise RuntimeError(f'Bad tree type(s): src={orig_tree_type},dst={tree_type}')
            elif node.category == Category.Moved:
                assert isinstance(node, FileToMove)
                orig_tree_type: int = node.src_node.identifier.tree_type
                if orig_tree_type == tree_type:
                    if tree_type == OBJ_TYPE_LOCAL_DISK:
                        cmd = MoveFileLocallyCommand(model_obj=node)
                    elif tree_type == OBJ_TYPE_GDRIVE:
                        cmd = MoveFileGDriveCommand(model_obj=node)
                    else:
                        raise RuntimeError(f'Bad tree type: {tree_type}')
                elif orig_tree_type == OBJ_TYPE_LOCAL_DISK and tree_type == OBJ_TYPE_GDRIVE:
                    cmd = UploadToGDriveCommand(model_obj=node)
                elif orig_tree_type == OBJ_TYPE_GDRIVE and tree_type == OBJ_TYPE_LOCAL_DISK:
                    cmd = DownloadFromGDriveCommand(model_obj=node)
                else:
                    raise RuntimeError(f'Bad tree type(s): src={orig_tree_type}, dst={tree_type}')
            elif node.category == Category.Deleted:
                if tree_type == OBJ_TYPE_LOCAL_DISK:
                    cmd = DeleteLocalFileCommand(model_obj=node)
                elif tree_type == OBJ_TYPE_GDRIVE:
                    cmd = DeleteGDriveFileCommand(model_obj=node)
                else:
                    raise RuntimeError(f'Bad tree type: {tree_type}')
            elif node.category == Category.Updated:
                assert isinstance(node, FileToUpdate)
                orig_tree_type = node.src_node.identifier.tree_type
                if orig_tree_type == tree_type:
                    if tree_type == OBJ_TYPE_LOCAL_DISK:
                        cmd = CopyFileLocallyCommand(model_obj=node, overwrite=True)
                    elif tree_type == OBJ_TYPE_GDRIVE:
                        raise RuntimeError(f'Bad tree type: {tree_type}')
                    else:
                        raise RuntimeError(f'Bad tree type: {tree_type}')
                elif orig_tree_type == OBJ_TYPE_LOCAL_DISK and tree_type == OBJ_TYPE_GDRIVE:
                    cmd = UploadToGDriveCommand(model_obj=node, overwrite=True)
                elif orig_tree_type == OBJ_TYPE_GDRIVE and tree_type == OBJ_TYPE_LOCAL_DISK:
                    cmd = DownloadFromGDriveCommand(model_obj=node, overwrite=True)
                else:
                    raise RuntimeError(f'Bad tree type(s): src={orig_tree_type},dst={tree_type}')
            else:
                raise RuntimeError(f'Unsupported category: {node.category}')

            cmd_list.append(cmd)
        return cmd_list
