import logging
from abc import ABC, abstractmethod
from typing import Optional

from store.gdrive.gdrive_whole_tree import GDriveWholeTree
from model.uid import UID
from model.user_op import UserOp, UserOpResult, UserOpStatus, UserOpType
from store.gdrive.gdrive_client import GDriveClient
from util.simple_tree import BaseNode

logger = logging.getLogger(__name__)


class CommandContext:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS CommandContext
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, staging_dir: str, backend, tree_id: str, needs_gdrive: bool):
        self.staging_dir: str = staging_dir
        self.cacheman = backend.cacheman
        if needs_gdrive:
            self.gdrive_client: Optional[GDriveClient] = self.cacheman.get_gdrive_client()
            # This will sync latest changes before returning, which will be somewhat slow but should keep us consistent
            self.gdrive_tree: Optional[GDriveWholeTree] = self.cacheman.sync_and_get_gdrive_master_tree(tree_id=tree_id)
        else:
            self.gdrive_client: Optional[GDriveClient] = None
            self.gdrive_tree: Optional[GDriveWholeTree] = None

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        pass


class Command(BaseNode, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    ABSTRACT CLASS Command

    Every command has an associated target node and a UserOp.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, op: UserOp):
        BaseNode.__init__(self, identifier=uid)
        assert op

        self.op: UserOp = op
        self.tag: str = f'{self.__class__.__name__}(cmd_uid={self.identifier}) op={op.op_type.name} ({op.op_uid}) ' \
                        f'tgt={self.op.src_node.node_identifier})'

    def get_description(self) -> str:
        # default
        return self.tag

    @property
    def type(self) -> UserOpType:
        return self.op.op_type

    @property
    def uid(self) -> UID:
        return self.identifier

    @abstractmethod
    def execute(self, context: CommandContext):
        pass

    @abstractmethod
    def get_total_work(self) -> int:
        """Return the total work needed to complete this task, as an integer for a progressbar widget"""
        return 0

    def needs_gdrive(self):
        return False

    def completed_without_error(self):
        status = self.status()
        return status == UserOpStatus.COMPLETED_OK or status == UserOpStatus.COMPLETED_NO_OP

    def status(self) -> UserOpStatus:
        if self.op.result:
            return self.op.result.status
        return UserOpStatus.NOT_STARTED

    def set_error_result(self, err) -> UserOpResult:
        result = UserOpResult(UserOpStatus.STOPPED_ON_ERROR, error=err)
        self.op.result = result
        return result

    def get_error(self):
        if self.op.result:
            return self.op.result.error
        return None

    def __repr__(self):
        # default
        return f'{self.__class__.__name__}(uid={self.identifier} status={self.status()} total_work={self.get_total_work()} ' \
               f'user_op={self.op})'


class DeleteNodeCommand(Command, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DeleteNodeCommand

    A Command which deletes the target node. If to_trash is true, it's more of a move/update.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, op: UserOp, to_trash: bool, delete_empty_parent: bool):
        Command.__init__(self, uid, op)
        assert op.op_type == UserOpType.RM
        self.to_trash = to_trash
        self.delete_empty_parent = delete_empty_parent
        self.tag = f'{self.__class__.__name__}(cmd_uid={self.identifier} op_uid={op.op_uid} tgt={self.op.src_node.uid} ' \
                   f'to_trash={self.to_trash} delete_empty_parent={self.delete_empty_parent})'

    def __repr__(self):
        return f'{self.__class__.__name__}(cmd_uid={self.identifier} status={self.status()} total_work={self.get_total_work()} ' \
               f'to_trash={self.to_trash} delete_empty_parent={self.delete_empty_parent} tgt={self.op.src_node.node_identifier}'


class TwoNodeCommand(Command, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TwoNodeCommand

    Same functionality as Command but with an additional "source" node. Its "target" node represents the destination node.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, op: UserOp):
        Command.__init__(self, uid, op)
        self.tag = f'{self.__class__.__name__}(cmd_uid={self.identifier} op_uid={op.op_uid} src={self.op.src_node.uid} ' \
                   f'dst={self.op.src_node.uid}'

    def __repr__(self):
        return f'{self.__class__.__name__}(cmd_uid={self.identifier} status={self.status()} total_work={self.get_total_work()}' \
               f' src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'


class CopyNodeCommand(TwoNodeCommand, ABC):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TwoNodeCommand

    A TwoNodeCommand which does a copy from src to tgt.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, uid: UID, op: UserOp, overwrite: bool):
        TwoNodeCommand.__init__(self, uid, op)
        self.overwrite = overwrite
        self.tag = f'{self.__class__.__name__}(cmd_uid={self.identifier} op_uid={op.op_uid} overwrite={self.overwrite} ' \
                   f'src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'

    def __repr__(self):
        return f'{self.__class__.__name__}(cmd_uid={self.identifier} status={self.status()} total_work={self.get_total_work()}' \
               f' overwrite={self.overwrite} src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'
