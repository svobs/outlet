import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import List, Optional

import treelib

from store.gdrive.client import GDriveClient
from model.uid import UID
from model.user_op import UserOp, UserOpType
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.node import Node

logger = logging.getLogger(__name__)


# ENUM CommandStatus
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CommandStatus(IntEnum):
    NOT_STARTED = 1
    EXECUTING = 2
    STOPPED_ON_ERROR = 8
    COMPLETED_NO_OP = 9
    COMPLETED_OK = 10


# CLASS CommandContext
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CommandContext:
    def __init__(self, staging_dir: str, app, tree_id: str, needs_gdrive: bool):
        self.staging_dir: str = staging_dir
        self.cacheman = app.cacheman
        if needs_gdrive:
            self.gdrive_client: Optional[GDriveClient] = self.cacheman.get_gdrive_client()
            # This will sync latest changes before returning, which will be somewhat slow but should keep us consistent
            self.gdrive_tree: Optional[GDriveWholeTree] = self.cacheman.get_synced_gdrive_master_tree(tree_id=tree_id)
        else:
            self.gdrive_client: Optional[GDriveClient] = None
            self.gdrive_tree: Optional[GDriveWholeTree] = None

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        pass


# CLASS CommandResult
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class CommandResult:
    def __init__(self, status: CommandStatus, error=None, to_upsert=None, to_delete=None):
        self.status = status
        self.error = error
        self.nodes_to_upsert: List[Node] = to_upsert
        self.nodes_to_delete: List[Node] = to_delete


# ABSTRACT CLASS Command
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Command(treelib.Node, ABC):
    """Every command has an associated target node and a UserOp."""
    def __init__(self, uid: UID, op: UserOp):
        treelib.Node.__init__(self, identifier=uid)

        self.op: UserOp = op
        self.result: Optional[CommandResult] = None
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
        return status == CommandStatus.COMPLETED_OK or status == CommandStatus.COMPLETED_NO_OP

    def status(self) -> CommandStatus:
        if self.result:
            return self.result.status
        return CommandStatus.NOT_STARTED

    def set_error_result(self, err) -> CommandResult:
        result = CommandResult(CommandStatus.STOPPED_ON_ERROR, error=err)
        self.result = result
        return result

    def get_error(self):
        if self.result:
            return self.result.error
        return None

    def __repr__(self):
        # default
        return f'{self.__class__.__name__}(uid={self.identifier} status={self.status()} total_work={self.get_total_work()} ' \
               f'user_op={self.op})'


# CLASS DeleteNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DeleteNodeCommand(Command, ABC):
    """A Command which deletes the target node. If to_trash is true, it's more of a move/update."""
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


# CLASS TwoNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TwoNodeCommand(Command, ABC):
    """Same functionality as Command but with an additional "source" node. Its "target" node represents the destination node."""
    def __init__(self, uid: UID, op: UserOp):
        Command.__init__(self, uid, op)
        self.tag = f'{self.__class__.__name__}(cmd_uid={self.identifier} op_uid={op.op_uid} src={self.op.src_node.uid} ' \
                   f'dst={self.op.src_node.uid}'

    def __repr__(self):
        return f'{self.__class__.__name__}(cmd_uid={self.identifier} status={self.status()} total_work={self.get_total_work()}' \
               f' src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'


# CLASS CopyNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CopyNodeCommand(TwoNodeCommand, ABC):
    """A TwoNodeCommand which does a copy from src to tgt"""
    def __init__(self, uid: UID, op: UserOp, overwrite: bool):
        TwoNodeCommand.__init__(self, uid, op)
        self.overwrite = overwrite
        self.tag = f'{self.__class__.__name__}(cmd_uid={self.identifier} op_uid={op.op_uid} overwrite={self.overwrite} ' \
                   f'src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'

    def __repr__(self):
        return f'{self.__class__.__name__}(cmd_uid={self.identifier} status={self.status()} total_work={self.get_total_work()}' \
               f' overwrite={self.overwrite} src={self.op.src_node.node_identifier} dst={self.op.dst_node.node_identifier})'
