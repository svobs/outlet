import logging
import os
import pathlib
from abc import ABC, abstractmethod
from typing import Optional, Union

from constants import FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT, IS_WINDOWS, LOCAL_DISK_ROOT_PATH
from logging_constants import SUPER_DEBUG_ENABLED
from model.user_op import UserOp, UserOpResult, UserOpStatus, UserOpCode

logger = logging.getLogger(__name__)


class CommandContext:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CommandContext
    Contains state variables and access to other engine components. To be reused for execution of each command.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, primary_staging_dir: str, secondary_mount_staging_dir_name: str, cacheman, update_meta_also: bool, use_strict_state_enforcement: bool):
        self.primary_staging_dir: str = primary_staging_dir
        self.secondary_mount_staging_dir_name: str = secondary_mount_staging_dir_name
        self.cacheman = cacheman

        self.update_meta_also: bool = update_meta_also
        """If true, any moves or copies of nodes should also copy the meta from src to dst"""

        self.use_strict_state_enforcement: bool = use_strict_state_enforcement
        """If true, raise exception if we see something unexpected in src and dst nodes, even if we could otherwise work around it"""

    def get_staging_dir_path(self, dst_path: str, only_if_not_primary: bool = False) -> Optional[str]:
        """
        Determines and returns the relevant staging directory for the given destination path, for a CP or MV operation.
        Because a CP is a non-atomic, potentially long-running operation, the copy is first done to a staging directory, and then "moved" to
        the dst location, which *is* a fast and atomic operation if done on the same volume. So if we are doing a CP or even an MV whose dst
        is not local, we need to make sure that we stage the file on the same volume as the dst, and for that we can create a "secondary"
        staging directory on that volume.

        :param dst_path: destination path
        :param only_if_not_primary: if false: checks if we need to use a secondary staging dir, and returns either that or the primary staging dir.
        If true: checks if we need to use a secondary staging dir, and if so returns that and if not returns None
        """
        if IS_WINDOWS:
            raise RuntimeError(f'get_staging_dir_path(): TODO: add Windows support!')

        ancestor = pathlib.Path(dst_path).parent
        while not ancestor.is_mount():
            ancestor = pathlib.Path(dst_path).parent

        if ancestor == LOCAL_DISK_ROOT_PATH:
            if only_if_not_primary:
                return None
            else:
                staging_dir = self.primary_staging_dir
        else:
            staging_dir = os.path.join(ancestor, self.secondary_mount_staging_dir_name)
            # TODO: keep track of secondary staging dirs by storing them in a DB

        if not os.path.exists(staging_dir):
            logger.info(f'Creating staging dir: "{staging_dir}"')
            try:
                os.makedirs(name=staging_dir, exist_ok=True)
            except Exception:
                logger.error(f'Exception while making staging dir: {staging_dir}')
                raise
        else:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Staging dir for dst "{dst_path}" = "{staging_dir}"')

        return staging_dir


class Command(ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS Command

    A platform-specific wrapper around a UserOp, to facilitate execution of the op.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, op: UserOp):
        assert op

        self.op: UserOp = op

    def get_description(self) -> str:
        """A brief summary of the command."""
        # default
        return f'{self.__class__.__name__} op={self.op}'

    @property
    def op_type(self) -> UserOpCode:
        return self.op.op_type

    @abstractmethod
    def execute(self, context: CommandContext) -> UserOpResult:
        pass

    def get_total_work(self) -> int:
        """Return the total work needed to complete this task, as an integer for a progressbar widget"""
        return FILE_META_CHANGE_TOKEN_PROGRESS_AMOUNT

    def completed_without_error(self) -> bool:
        status = self.get_status()
        return status == UserOpStatus.COMPLETED_OK or status == UserOpStatus.COMPLETED_NO_OP

    def get_status(self) -> UserOpStatus:
        return self.op.get_status()

    def set_error_result(self, err: Union[str, Exception]) -> UserOpResult:
        result = UserOpResult(UserOpStatus.STOPPED_ON_ERROR, error=err)
        self.op.result = result
        return result

    def get_error(self):
        if self.op.result:
            return self.op.result.error
        return None

    def __repr__(self):
        """For debugging."""
        # default
        return f'{self.__class__.__name__} total_work={self.get_total_work()} op={self.op})'


class DeleteNodeCommand(Command, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DeleteNodeCommand

    A Command which deletes the src node. If to_trash is true, it's more of a move/update.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, op: UserOp, to_trash: bool):
        Command.__init__(self, op)
        assert op.op_type == UserOpCode.RM
        self.to_trash = to_trash

    def get_description(self) -> str:
        """A brief summary of the command."""
        return f'{self.__class__.__name__} op_uid={self.op.op_uid} tgt={self.op.src_node.dn_uid} to_trash={self.to_trash})'

    def __repr__(self):
        return f'{self.__class__.__name__} to_trash={self.to_trash} total_work={self.get_total_work()} op={self.op})'


class TwoNodeCommand(Command, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TwoNodeCommand

    Same functionality as Command but indicates that its operation has both a src and dst node.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, op: UserOp):
        Command.__init__(self, op)
        self.tag = f'{self.__class__.__name__} op_uid={op.op_uid} src={self.op.src_node.uid} dst={self.op.src_node.uid}'

    def __repr__(self):
        return f'{self.__class__.__name__} status={self.get_status()} total_work={self.get_total_work()}' \
               f' src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'


class CopyNodeCommand(TwoNodeCommand, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CopyNodeCommand

    A TwoNodeCommand which does a copy from src to dst.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, op: UserOp, overwrite: bool):
        TwoNodeCommand.__init__(self, op)
        self.overwrite = overwrite
        self.tag = f'{self.__class__.__name__} op_uid={op.op_uid} overwrite={self.overwrite} ' \
                   f'src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'

    def __repr__(self):
        return f'{self.__class__.__name__} status={self.get_status()} total_work={self.get_total_work()}' \
               f' overwrite={self.overwrite} src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'


class FinishCopyToDirCommand(TwoNodeCommand, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS FinishCopyToDirCommand

    A TwoNodeCommand which finishes copying meta from src dir to dst dir, and then (if delete_src_node_after==True) deletes src).
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, op: UserOp, delete_src_node_after: bool):
        TwoNodeCommand.__init__(self, op)
        self.delete_src_node_after = delete_src_node_after
        self.tag = f'{self.__class__.__name__} op_uid={op.op_uid} delete_src_node_after={self.delete_src_node_after} ' \
                   f'src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'

    def __repr__(self):
        return f'{self.__class__.__name__} status={self.get_status()} total_work={self.get_total_work()}' \
               f' delete_src_node_after={self.delete_src_node_after} src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'
