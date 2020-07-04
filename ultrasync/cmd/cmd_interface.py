import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import List

import treelib

from gdrive.client import GDriveClient
from index.uid.uid import UID
from model.change_action import ChangeAction, ChangeType
from model.gdrive_whole_tree import GDriveWholeTree
from model.node.display_node import DisplayNode

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
    def __init__(self, staging_dir: str, application, tree_id: str, needs_gdrive: bool):
        self.staging_dir = staging_dir
        self.config = application.config
        self.cache_manager = application.cache_manager
        self.uid_generator = application.uid_generator
        if needs_gdrive:
            self.gdrive_client = GDriveClient(application=application, tree_id=None)
            self.gdrive_tree: GDriveWholeTree = self.cache_manager.get_gdrive_whole_tree(tree_id=tree_id)


# CLASS CommandResult
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
class CommandResult:
    def __init__(self, status: CommandStatus, error=None, to_upsert=None, to_delete=None):
        self.status = status
        self.error = error
        self.nodes_to_upsert: List[DisplayNode] = to_upsert
        self.nodes_to_delete: List[DisplayNode] = to_delete


# ABSTRACT CLASS Command
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Command(treelib.Node, ABC):
    """Every command has an associated target node and a ChangeAction."""
    def __init__(self, uid: UID, change_action: ChangeAction):
        treelib.Node.__init__(self, identifier=uid)

        self.change_action: ChangeAction = change_action
        self.result: CommandResult = None
        self.tag: str = f'{__class__.__name__}(uid={self.identifier})'

    def get_description(self) -> str:
        # default
        return self.tag

    @property
    def type(self) -> ChangeType:
        return self.change_action.change_type

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
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, status={self.status()}, ' \
               f'change_actions={self.change_action}'


# CLASS DeleteNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DeleteNodeCommand(Command, ABC):
    """A Command which deletes the target node. If to_trash is true, it's more of a move/update."""
    def __init__(self, uid: UID, change_action: ChangeAction, to_trash: bool, delete_empty_parent: bool):
        Command.__init__(self, uid, change_action)
        assert change_action.change_type == ChangeType.RM
        self.to_trash = to_trash
        self.delete_empty_parent = delete_empty_parent


# CLASS TwoNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TwoNodeCommand(Command, ABC):
    """Same functionality as Command but with an additional "source" node. Its "target" node represents the destination node."""
    def __init__(self, uid: UID, change_action: ChangeAction):
        Command.__init__(self, uid, change_action)


# CLASS CopyNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CopyNodeCommand(TwoNodeCommand, ABC):
    """A TwoNodeCommand which does a copy from src to tgt"""
    def __init__(self, uid: UID, change_action: ChangeAction, overwrite: bool):
        TwoNodeCommand.__init__(self, uid, change_action)
        self.overwrite = overwrite
