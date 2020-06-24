import collections
import logging
import time
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Deque, List, Optional

import treelib

from command.change_action import ChangeAction, ChangeType
from gdrive.client import GDriveClient
from index.uid import UID
from model.display_node import DisplayNode
from model.gdrive_whole_tree import GDriveWholeTree
from model.goog_node import GoogNode

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

    def resolve_parent_ids_to_goog_ids(self, node: GoogNode) -> str:
        parent_uids: List[UID] = node.get_parent_uids()
        if not parent_uids:
            raise RuntimeError(f'Parents are required but item has no parents: {node}')

        # This will raise an exception if it cannot resolve:
        parent_goog_ids: List[str] = self.gdrive_tree.resolve_uids_to_goog_ids(parent_uids)

        if len(parent_goog_ids) == 0:
            raise RuntimeError(f'No parent Google IDs for: {node}')
        if len(parent_goog_ids) > 1:
            # not supported at this time
            raise RuntimeError(f'Too many parent Google IDs for: {node}')

        parent_goog_id: str = parent_goog_ids[0]
        return parent_goog_id


# ABSTRACT CLASS Command
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class Command(treelib.Node, ABC):
    """Every command has an associated target node and a ChangeAction."""
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode):
        treelib.Node.__init__(self, identifier=uid)

        self._change_action = change_action
        self.tgt_node = tgt_node
        """If the ChangeAction uses a dst_uid, this will be it. Otherwise it will use a src_uid"""

        self._status = CommandStatus.NOT_STARTED
        self._error = None
        self.tag = f'{__class__.__name__}(uid={self.identifier})'

    @property
    def uid(self):
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
        return self._status == CommandStatus.COMPLETED_OK or self._status == CommandStatus.COMPLETED_NO_OP

    def status(self) -> CommandStatus:
        return self._status

    def set_error(self, err):
        self._error = err
        self._status = CommandStatus.STOPPED_ON_ERROR

    def get_error(self):
        return self._error

    def __repr__(self):
        return f'{__class__.__name__}(uid={self.identifier}, total_work={self.get_total_work()}, status={self._status}, model={self.tgt_node}'


# CLASS DeleteNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DeleteNodeCommand(Command, ABC):
    """A Command which deletes the target node. If to_trash is true, it's more of a move/update."""
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, to_trash: bool, delete_empty_parent: bool):
        Command.__init__(self, uid, change_action, tgt_node)
        assert change_action.change_type == ChangeType.RM
        self.to_trash = to_trash
        self.delete_empty_parent = delete_empty_parent


# CLASS TwoNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TwoNodeCommand(Command, ABC):
    """Same functionality as Command but with an additional "source" node. Its "target" node represents the destination node."""
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, src_node: DisplayNode):
        Command.__init__(self, uid, change_action, tgt_node)
        self.src_node = src_node


# CLASS CopyNodeCommand
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CopyNodeCommand(TwoNodeCommand, ABC):
    """A TwoNodeCommand which does a copy from src to tgt"""
    def __init__(self, uid: UID, change_action: ChangeAction, tgt_node: DisplayNode, src_node: DisplayNode, overwrite: bool):
        TwoNodeCommand.__init__(self, uid, change_action, tgt_node)
        self.overwrite = overwrite


# CLASS CommandBatch
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class CommandBatch:
    def __init__(self, uid: UID, cmd_tree):
        self.uid: UID = uid
        self.create_ts = int(time.time())
        self.tree: treelib.Tree = cmd_tree

    def get_breadth_first_list(self):
        """Returns the command tree as a list, in breadth-first order"""
        blist: List[Command] = []

        queue: Deque[Command] = collections.deque()
        # skip root:
        for child in self.tree.children(self.tree.root):
            queue.append(child)

        while len(queue) > 0:
            item: Command = queue.popleft()
            blist.append(item)
            for child in self.tree.children(item.identifier):
                queue.append(child)

        return blist

    def __len__(self):
        # subtract root node
        return self.tree.__len__() - 1

    def get_item_for_uid(self, uid: UID) -> Command:
        return self.tree.get_node(uid)

    def get_total_completed(self) -> int:
        """Returns the number of commands which executed successfully"""
        total_succeeded: int = 0
        for command in self.get_breadth_first_list():
            if command.completed_without_error():
                total_succeeded += 1
        return total_succeeded

    def get_parent(self, uid: UID) -> Optional[Command]:
        parent = self.tree.parent(nid=uid)
        if parent and isinstance(parent, Command):
            return parent
        return None

