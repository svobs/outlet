import collections
import logging
import os
import threading
import unittest
from functools import partial
from typing import Callable, Deque, Iterable, List, Optional, Tuple

from py7zr import SevenZipFile
from pydispatch import dispatcher

from app.backend_integrated import BackendIntegrated
from app.gtk_frontend import OutletApplication
from app_config import AppConfig
from command.cmd_interface import Command
from constants import OPS_FILE_NAME
from store import cache_manager
from store.sqlite.op_db import OpDatabase
from model.uid import UID
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node, SPIDNodePair
from ui.signal import ID_CENTRAL_EXEC, ID_LEFT_TREE, ID_RIGHT_TREE, Signal
from ui.tree import root_path_config
from ui.tree.controller import TreePanelController
from util import file_util

LOAD_TIMEOUT_SEC = 6000
ENABLE_CHANGE_EXECUTION_THREAD = True

TEST_BASE_DIR = file_util.get_resource_path('test')
TEST_ARCHIVE = 'ChangeTest.7z'
TEST_ARCHIVE_PATH = os.path.join(TEST_BASE_DIR, TEST_ARCHIVE)
TEST_TARGET_DIR = os.path.join(TEST_BASE_DIR, 'ChangeTest')

logger = logging.getLogger(__name__)


class FNode:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    MOCK CLASS FNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, name: str, size_bytes: int):
        self.name: str = name
        self.size_bytes: int = size_bytes

    @classmethod
    def is_dir(cls):
        return False

    def __repr__(self):
        return f'File("{self.name}" size={self.size_bytes})'


class DNode(FNode):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    MOCK CLASS DNode
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, name: str, size_bytes: int, children: Optional[List] = None):
        super().__init__(name, size_bytes)
        if children is None:
            children = list()
        self.children: List[FNode] = children

    @classmethod
    def is_dir(cls):
        return True

    def __repr__(self):
        return f'Dir("{self.name}" size={self.size_bytes} children={len(self.children)})'


# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# Static stuff
# ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

INITIAL_LOCAL_TREE_LEFT = [
    FNode('American_Gothic.jpg', 2061397),
    FNode('Angry-Clown.jpg', 824641),
    DNode('Art', (88259 + 652220 + 239739 + 44487 + 479124) + (147975 + 275771 + 8098 + 247023 + 36344), [
        FNode('Dark-Art.png', 147975),
        FNode('Hokusai_Great-Wave.jpg', 275771),
        DNode('Modern', (88259 + 652220 + 239739 + 44487 + 479124), [
            FNode('1923-art.jpeg', 88259),
            FNode('43548-forbidden_planet.jpg', 652220),
            FNode('Dunno.jpg', 239739),
            FNode('felix-the-cat.jpg', 44487),
            FNode('Glow-Cat.png', 479124),
        ]),
        FNode('Mona-Lisa.jpeg', 8098),
        FNode('william-shakespeare.jpg', 247023),
        FNode('WTF.jpg', 36344),
    ]),
    FNode('Egypt.jpg', 154564),
    FNode('George-Floyd.png', 27601),
    FNode('Geriatric-Clown.jpg', 89182),
    FNode('Keep-calm-and-carry-on.jpg', 745698),
]

INITIAL_LOCAL_TREE_RIGHT = [
    FNode('Edvard-Munch-The-Scream.jpg', 114082),
    FNode('M83.jpg', 17329),
    FNode('oak-tree-sunset.jpg', 386888),
    FNode('Ocean-Wave.jpg', 83713),
    FNode('Starry-Night.jpg', 91699),
    FNode('we-can-do-it-poster.jpg', 390093),
]


def get_name_lower(display_node: Node):
    return display_node.name.lower()


def do_and_wait_for_signal(action_func, signal, tree_id):
    received = threading.Event()

    def on_received():
        logger.debug(f'Received signal: {signal} from tree {tree_id}')
        received.set()

    dispatcher.connect(signal=signal, receiver=on_received, sender=tree_id)

    action_func()
    logger.debug(f'Waiting for signal: {signal} from tree {tree_id}')
    if not received.wait(LOAD_TIMEOUT_SEC):
        raise RuntimeError(f'Timed out waiting for signal: {signal} from tree: {tree_id}')


def name_equals_func(node_name, node) -> bool:
    if logger.isEnabledFor(logging.DEBUG) and not node.is_ephemereal():
        logger.debug(f'Examining node uid={node.uid} name={node.name} (looking for: {node_name})')
    return not node.is_ephemereal() and node.name == node_name


class OpTestBase(unittest.TestCase):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OpTestBase
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def setUp(self):
        self.op_db_path = None
        self.app = None
        self.backend = None
        self.app_thread = None
        self.left_con = None
        self.right_con = None

        self.left_tree_initial = None
        self.left_tree_type = None
        self.left_tree_root_path = None
        self.left_tree_root_uid = None

        self.right_tree_initial = None
        self.right_tree_type: Optional[int] = None
        self.right_tree_root_path: Optional[str] = None
        self.right_tree_root_uid: Optional[UID] = None

    def do_setup(self, do_before_verify_func: Callable = None):
        # Remove test files and replace with freshly extracted files
        if os.path.exists(TEST_TARGET_DIR):
            file_util.rm_tree(TEST_TARGET_DIR)
            logger.debug(f'Deleted dir: {TEST_TARGET_DIR}')

        with SevenZipFile(file=TEST_ARCHIVE_PATH) as archive:
            archive.extractall(TEST_BASE_DIR)
        logger.debug(f'Extracted: {TEST_ARCHIVE_PATH} to {TEST_BASE_DIR}')

        config = AppConfig()

        # Delete ops cache, so that prev run doesn't contaminate us:
        cache_dir_path = cache_manager.ensure_cache_dir_path(config)
        self.op_db_path = os.path.join(cache_dir_path, OPS_FILE_NAME)
        if os.path.exists(self.op_db_path):
            file_util.rm_file(self.op_db_path)

        config.write(root_path_config.make_tree_type_config_key(ID_LEFT_TREE), self.left_tree_type)
        config.write(root_path_config.make_root_path_config_key(ID_LEFT_TREE), self.left_tree_root_path)
        if self.left_tree_root_uid:
            config.write(root_path_config.make_root_uid_config_key(ID_LEFT_TREE), self.left_tree_root_uid)

        config.write(root_path_config.make_tree_type_config_key(ID_RIGHT_TREE), self.right_tree_type)
        config.write(root_path_config.make_root_path_config_key(ID_RIGHT_TREE), self.right_tree_root_path)
        if self.right_tree_root_uid:
            config.write(root_path_config.make_root_uid_config_key(ID_RIGHT_TREE), self.right_tree_root_uid)

        backend = BackendIntegrated(config)
        self.app = OutletApplication(config, backend)
        # Disable execution so we can study the state of the OpGraph:
        self.backend.executor.enable_op_execution_thread = False

        load_left_done = threading.Event()
        load_right_done = threading.Event()

        def run_thread():
            # this starts the executor, which inits the CacheManager
            # This does not return until the program exits
            self.app.run([])

        def after_left_tree_loaded(sender):
            logger.debug(f'Received signal: "{Signal.LOAD_UI_TREE_DONE}" for "{sender}"')
            load_left_done.set()

        def after_right_tree_loaded(sender):
            logger.debug(f'Received signal: "{Signal.LOAD_UI_TREE_DONE}" for "{sender}"')
            load_right_done.set()

        dispatcher.connect(signal=Signal.LOAD_UI_TREE_DONE, sender=ID_LEFT_TREE, receiver=after_left_tree_loaded)
        dispatcher.connect(signal=Signal.LOAD_UI_TREE_DONE, sender=ID_RIGHT_TREE, receiver=after_right_tree_loaded)
        self.app_thread = threading.Thread(target=run_thread, daemon=True, name='AppTestRunnerThread')
        self.app_thread.start()

        # wait for both sides to load before returning:
        if not load_left_done.wait(LOAD_TIMEOUT_SEC):
            raise RuntimeError('Timed out waiting for left to load!')
        if not load_right_done.wait(LOAD_TIMEOUT_SEC):
            raise RuntimeError('Timed out waiting for right to load!')
        self.left_con: TreePanelController = self.app.get_tree_controller(ID_LEFT_TREE)
        self.right_con: TreePanelController = self.app.get_tree_controller(ID_RIGHT_TREE)

        if do_before_verify_func:
            do_before_verify_func()

        self.verify(self.left_con, self.left_tree_initial)
        self.verify(self.right_con, self.right_tree_initial)
        logger.warning(f'LOAD COMPLETE')

    def tearDown(self) -> None:
        with OpDatabase(self.op_db_path, self.backend) as op_db:
            op_list = op_db.get_all_pending_ops()
            self.assertEqual(0, len(op_list), 'We have ops remaining after quit!')

        logger.info('Quitting app!')
        self.app.quit()
        self.left_con.shutdown()
        self.right_con.shutdown()
        del self.left_con
        del self.right_con
        del self.app
        self.app_thread.join(LOAD_TIMEOUT_SEC)
        del self.app_thread

    def verify_one_memstore_dir(self, tree_con, expected: List[FNode], actual: Iterable[Node],
                                dir_deque: Deque[Tuple[List[FNode], Iterable[Node]]]):
        actual_iter = iter(actual)
        for i in range(0, len(expected)):
            expected_node: FNode = expected[i]
            try:
                actual_node: Node = next(actual_iter)
                logger.info(f'Examining: {actual_node} (expecting: {expected_node})')
                if actual_node.name != expected_node.name:
                    logger.debug('XXX')
                self.assertEqual(expected_node.name, actual_node.name)
                self.assertEqual(expected_node.size_bytes, actual_node.get_size_bytes())
                self.assertEqual(expected_node.is_dir(), actual_node.is_dir())
                logger.info(f'OK: {expected_node.name}')

                if expected_node.is_dir():
                    assert isinstance(expected_node, DNode)
                    expected_list: List[FNode] = expected_node.children
                    actual_list: Iterable[Node] = tree_con.get_tree().get_children(actual_node)
                    dir_deque.append((expected_list, actual_list))

            except StopIteration:
                self.fail(f'Tree "{tree_con.tree_id}" is missing node: {expected_node}')

        try:
            actual_node: Node = next(actual_iter)
            self.fail(f'Tree "{tree_con.tree_id}" has unexpected node: {actual_node}')
        except StopIteration:
            pass

    def verify_one_display_dir(self, tree_con, expected: List[FNode], actual: List[Node],
                               dir_deque: Deque[Tuple[List[FNode], Iterable[Node]]]):
        """Displayed directories may not all be loaded. But we will verify the ones that are"""
        actual_iter = iter(actual)
        for i in range(0, len(expected)):
            expected_node: FNode = expected[i]
            try:
                actual_node: Node = next(actual_iter)
                logger.info(f'Examining: {actual_node} (expecting: {expected_node})')
                self.assertEqual(expected_node.name, actual_node.name)
                self.assertEqual(expected_node.size_bytes, actual_node.get_size_bytes(), f'For expected node: {expected_node}')
                self.assertEqual(expected_node.is_dir(), actual_node.is_dir())
                logger.info(f'OK: {expected_node.name}')

                if expected_node.is_dir():
                    assert isinstance(expected_node, DNode)
                    expected_list: List[FNode] = expected_node.children
                    actual_list: Iterable[Node] = tree_con.display_store.get_displayed_children_of(actual_node.uid)
                    if actual_list:
                        dir_deque.append((expected_list, actual_list))

            except StopIteration:
                self.fail(f'Tree "{tree_con.tree_id}" is not displaying expected node: {expected_node}')

        try:
            actual_node: Node = next(actual_iter)
            self.fail(f'Tree "{tree_con.tree_id}" has unexpected displayed node: {actual_node}')
        except StopIteration:
            pass

    def verify(self, tree_con: TreePanelController, expected_list_root: List[FNode]):
        logger.info(f'Verifying "{tree_con.tree_id}"')
        tree_con.get_tree().print_tree_contents_debug()

        # Verify that all nodes loaded correctly into the cache, which will be reflected by the state of the DisplayTree:
        backing_tree: DisplayTree = tree_con.get_tree()

        dir_deque: Deque[Tuple[List[FNode], Iterable[Node]]] = collections.deque()
        """Each entry contains the expected and actual contents of a single dir"""

        logger.info(f'Verifying nodes in memstore for "{tree_con.tree_id}"...')
        actual_list: Iterable[Node] = backing_tree.get_children_for_root()

        # Cached nodes (in tree model)
        count_dir = 0
        dir_deque.append((expected_list_root, actual_list))
        while len(dir_deque) > 0:
            count_dir += 1
            expected_list, actual_list = dir_deque.popleft()
            # sort the actual list by name, since it is not required to be sorted
            actual_list: List[Node] = list(actual_list)
            actual_list.sort(key=get_name_lower)
            self.verify_one_memstore_dir(tree_con, expected_list, actual_list, dir_deque)

        logger.info(f'Verified {count_dir} dirs in memstore for "{tree_con.tree_id}"')

        # Displayed nodes
        logger.info(f'Verifying nodes in display tree for "{tree_con.tree_id}"...')
        count_dir = 0
        actual_list: List[Node] = tree_con.display_store.get_displayed_children_of(None)
        dir_deque.append((expected_list_root, actual_list))
        while len(dir_deque) > 0:
            count_dir += 1
            expected_list, actual_list = dir_deque.popleft()
            # sort the actual list by name, since it is not required to be sorted
            actual_list: List[Node] = list(actual_list)
            actual_list.sort(key=get_name_lower)
            self.verify_one_display_dir(tree_con, expected_list, actual_list, dir_deque)
        logger.info(f'Verified {count_dir} display dirs for "{tree_con.tree_id}"')

    def find_iter_by_name(self, tree_con, node_name: str):
        name_equals_func_bound: Callable = partial(name_equals_func, node_name)
        tree_iter = tree_con.display_store.find_in_tree(name_equals_func_bound)
        if not tree_iter:
            self.fail(f'Expected to find node named "{node_name}"')
        return tree_iter

    def find_iter_by_name_in_left_tree(self, node_name):
        return self.find_iter_by_name(self.left_con, node_name)

    def find_node_by_name(self, tree_con, node_name: str) -> SPIDNodePair:
        tree_iter = self.find_iter_by_name(tree_con, node_name)
        sn = tree_con.display_store.build_sn_from_tree_path(tree_iter)
        logger.info(f'Found "{sn.node.name}"')
        return sn

    def find_node_by_name_in_left_tree(self, node_name) -> SPIDNodePair:
        return self.find_node_by_name(self.left_con, node_name)

    def do_and_verify(self, do_func: Callable, count_expected_cmds: int, wait_for_left: bool, wait_for_right: bool,
                      expected_left: List, expected_right: List):
        # Drag & drop 1 node, which represents a tree of 10 files and 2 dirs:
        completed_cmds: List[Command] = []
        all_commands_complete = threading.Event()
        left_stats_updated = threading.Event()
        right_stats_updated = threading.Event()

        def on_stats_updated(sender):
            logger.info(f'Got signal: {Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE} for "{sender}"')
            if sender == self.left_con.tree_id:
                left_stats_updated.set()
            elif sender == self.right_con.tree_id:
                right_stats_updated.set()

        def on_command_complete(sender, command: Command):
            completed_cmds.append(command)
            logger.info(f'Got a completed command (total: {len(completed_cmds)}, expecting: {count_expected_cmds})')
            if len(completed_cmds) >= count_expected_cmds:
                all_commands_complete.set()
                # Now start waiting for "stats updated" Signal. Do not do before, because this signal can be sent any time there is a long-running op
                dispatcher.connect(signal=Signal.REFRESH_SUBTREE_STATS_COMPLETELY_DONE, receiver=on_stats_updated)

        dispatcher.connect(signal=Signal.COMMAND_COMPLETE, receiver=on_command_complete)

        do_func()

        dispatcher.send(signal=Signal.RESUME_OP_EXECUTION, sender=ID_CENTRAL_EXEC)
        logger.info('Sleeping until we get what we want')
        if not all_commands_complete.wait(LOAD_TIMEOUT_SEC):
            raise RuntimeError('Timed out waiting for all commands to complete!')
        if wait_for_left:
            if not left_stats_updated.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for Left stats to update!')
        if wait_for_right:
            if not right_stats_updated.wait(LOAD_TIMEOUT_SEC):
                raise RuntimeError('Timed out waiting for Right stats to update!')
        # logger.info('Sleeping')
        # time.sleep(10)  # in seconds

        self.verify(self.left_con, expected_left)
        self.verify(self.right_con, expected_right)
        logger.info('Done!')

    def expand_visible_node(self, tree_con, node_name: str):
        # Need to first expand tree in order to find child nodes. This func does nothing if the row is already expanded
        tree_iter = self.find_iter_by_name(tree_con, node_name)
        tree_path = tree_con.display_store.model.get_path(tree_iter)

        if not tree_con.tree_view.row_expanded(tree_path):
            def action_func():
                tree_con.tree_view.expand_row(path=tree_path, open_all=True)

            do_and_wait_for_signal(action_func, Signal.NODE_EXPANSION_DONE, tree_con.tree_id)
