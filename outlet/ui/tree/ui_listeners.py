import logging
from typing import Iterable, List, Optional

import gi
from pydispatch import dispatcher

import ui.actions as actions
from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK, TREE_TYPE_MIXED, TreeDisplayMode
from diff.change_maker import ChangeMaker, SPIDNodePair
from model.node.local_disk_node import LocalFileNode
from model.node.node import Node
from model.node_identifier import SinglePathNodeIdentifier
from model.user_op import UserOp, UserOpType
from model.uid import UID
from ui.tree.context_menu import TreeContextMenu
from ui.tree.controller import TreePanelController
from util.has_lifecycle import HasLifecycle

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gdk, Gtk

logger = logging.getLogger(__name__)


# CLASS DragAndDropData
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class DragAndDropData:
    def __init__(self, dd_uid: UID, src_treecon: TreePanelController, sn_list: List[SPIDNodePair]):
        self.dd_uid: UID = dd_uid
        self.src_treecon: TreePanelController = src_treecon
        self.sn_list: List[SPIDNodePair] = sn_list


# CLASS TreeUiListeners
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class TreeUiListeners(HasLifecycle):
    def __init__(self, config, controller):
        HasLifecycle.__init__(self)
        self.con = controller
        self._ui_enabled = True
        self._drag_data: Optional[DragAndDropData] = None
        self._drop_data = None
        self._connected_eids = []
        self._context_menus_by_type = {TREE_TYPE_LOCAL_DISK: TreeContextMenu(self.con),
                                       TREE_TYPE_GDRIVE: TreeContextMenu(self.con),
                                       TREE_TYPE_MIXED: None}  # TODO: handle mixed

    def init(self):
        logger.debug(f'[{self.con.tree_id}] TreeUiListeners init')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(actions.TOGGLE_UI_ENABLEMENT, self._on_enable_ui_toggled)

        targeted_signals: List[str] = []
        general_signals: List[str] = [actions.TOGGLE_UI_ENABLEMENT]

        if self.con.cacheman.reload_tree_on_root_path_update:
            self.connect_dispatch_listener(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=self.con.tree_id)
            targeted_signals.append(actions.ROOT_PATH_UPDATED)

        if self.con.cacheman.load_all_caches_on_startup or self.con.cacheman.load_caches_for_displayed_trees_at_startup:
            logger.debug(f'[{self.con.tree_id}] LoadAllAtStartup={self.con.cacheman.load_all_caches_on_startup}, '
                         f'LoadDisplayedAtStartup={self.con.cacheman.load_caches_for_displayed_trees_at_startup}')
            # Either enabled for this tree to be loaded automatically
            self.connect_dispatch_listener(signal=actions.START_CACHEMAN_DONE, receiver=self._after_all_caches_loaded)
            if self.con.app.cacheman.load_all_caches_done:
                # If cacheman finished loading before we even started listening, just execute here.
                # Possible race condition? Should be ok for CPython...
                # FIXME: this is nasty. Find a better solution. Something like a queuing solution for signals...
                self._after_all_caches_loaded(self.con.tree_id)
            general_signals.append(actions.START_CACHEMAN_DONE)

        # Status bar
        self.connect_dispatch_listener(signal=actions.SET_STATUS, receiver=self._on_set_status, sender=self.con.tree_id)
        targeted_signals.append(actions.SET_STATUS)

        logger.debug(f'[{self.con.tree_id}] Listening for signals: Any={general_signals}, "{self.con.tree_id}"={targeted_signals}')

        # TreeView
        # double-click or enter key:
        eid = self.con.tree_view.connect("row-activated", self._on_row_activated, self.con.tree_id)
        self._connected_eids.append(eid)
        # right-click:
        eid = self.con.tree_view.connect('button-press-event', self._on_tree_button_press, self.con.tree_id)
        self._connected_eids.append(eid)
        # other keys like 'Del'
        eid = self.con.tree_view.connect('key-press-event', self._on_key_press, self.con.tree_id)
        self._connected_eids.append(eid)
        # user clicked on the expand
        eid = self.con.tree_view.connect('row-expanded', self._on_toggle_gtk_row_expanded_state, True)
        self._connected_eids.append(eid)
        eid = self.con.tree_view.connect('row-collapsed', self._on_toggle_gtk_row_expanded_state, False)
        self._connected_eids.append(eid)
        # select.connect("changed", self._on_tree_selection_changed)

        if self.con.treeview_meta.can_modify_tree:
            action_mask = Gdk.DragAction.DEFAULT | Gdk.DragAction.MOVE | Gdk.DragAction.COPY
            self.con.tree_view.enable_model_drag_source(Gdk.ModifierType.BUTTON1_MASK, [], action_mask)
            self.con.tree_view.enable_model_drag_dest([], action_mask)
            # Text targets:
            self.con.tree_view.drag_dest_set_target_list(None)
            self.con.tree_view.drag_source_set_target_list(None)
            self.con.tree_view.drag_dest_add_text_targets()
            self.con.tree_view.drag_source_add_text_targets()
            self.con.tree_view.connect("drag-data-received", self._drag_data_received)
            self.con.tree_view.connect("drag-data-get", self._drag_data_get)
            # FIXME Want to remove highlight when dropping in non-dir rows. But this is not the correct way to do this.
            # self.con.tree_view.connect('drag-motion', self._on_drag_motion)

            self.connect_dispatch_listener(signal=actions.DRAG_AND_DROP, receiver=self._receive_drag_data_signal)
            self.connect_dispatch_listener(signal=actions.DRAG_AND_DROP_DIRECT, receiver=self._do_drop, sender=self.con.tree_id)
            # ^^^ mostly for testing

    def disconnect_gtk_listeners(self):
        for eid in self._connected_eids:
            self.con.tree_view.disconnect(eid)
        self._connected_eids.clear()

    # LISTENERS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    def _on_drag_motion(self, treeview, drag_context, x, y, time):
        # FIXME see above
        tree_path, col, cellx, celly = treeview.get_path_at_pos(x, y)
        node = treeview.get_model()[tree_path][self.con.treeview_meta.col_num_data]
        logger.debug(f'Node: {node}')

        drop_info = treeview.get_dest_row_at_pos(x, y)
        if drop_info:
            tree_path, drop_position = drop_info

            if drop_position == Gtk.TreeViewDropPosition.INTO_OR_BEFORE or drop_position == Gtk.TreeViewDropPosition.INTO_OR_AFTER:
                is_into = True
            else:
                is_into = False

            logger.debug(f'IsInto: {is_into}, IsDir: {node.is_dir()}')
            if node:
                if is_into and not node.is_dir():
                    return True
        # False == allow drop
        return False

    def _drag_data_get(self, treeview, drag_context, selection_data, target_id, etime):
        """Drag & Drop 1/4: collect and send data and signal from source"""
        selected_sn_list: List[SPIDNodePair] = self.con.display_store.get_multiple_selection_sn_list()
        if selected_sn_list:
            # Avoid complicated, undocumented GTK3 garbage by just sending a UID along with needed data via the dispatcher. See _check_drop()
            dd_uid = self.con.parent_win.app.uid_generator.next_uid()
            action = drag_context.get_selected_action()  # TODO: add support for more actions. For now, assume CP
            drag_data = DragAndDropData(dd_uid, self.con, selected_sn_list)
            dispatcher.send(signal=actions.DRAG_AND_DROP, sender=self.con.tree_id, data=drag_data)
            selection_data.set_text(str(dd_uid), -1)
        else:
            selection_data.set_text('', -1)

    def _receive_drag_data_signal(self, sender, data: DragAndDropData):
        """Drag & Drop 2 or 3 /4: receive drag data at dest"""
        logger.debug(f'[{self.con.tree_id}] Received signal: "{actions.DRAG_AND_DROP}"')
        self._drag_data = data
        self._check_drop()

    def _drag_data_received(self, treeview, context, x, y, selection: Gtk.SelectionData, info, etime):
        """Drag & Drop 2 or 3 /4: receive drop GTK signal"""
        text: str = selection.get_text()
        if not text:
            return
        dd_uid = UID(text)

        drop_info = treeview.get_dest_row_at_pos(x, y)
        if drop_info:
            tree_path, drop_position = drop_info
            if drop_position == Gtk.TreeViewDropPosition.INTO_OR_BEFORE or drop_position == Gtk.TreeViewDropPosition.INTO_OR_AFTER:
                is_into = True
            else:
                is_into = False
        else:
            logger.info('No drop info! Assuming a top-level drop')
            is_into = False
            tree_path = None
        self._drop_data = dd_uid, tree_path, is_into
        self._check_drop()

    def _do_drop(self, sender, drag_data: DragAndDropData, tree_path: Gtk.TreePath, is_into: bool):
        # Puts the drag data into/adjacent to the given tree_path.
        logger.info(f'[{self.con.tree_id}] We received a drop of {len(drag_data.sn_list)} nodes!')

        if tree_path:
            sn_dst: SPIDNodePair = self.con.display_store.build_sn_from_tree_path(tree_path)
        else:
            # Assume we are dropping into the tree root
            is_into = True
            sn_dst = SPIDNodePair(self.con.get_tree().root_identifier, self.con.get_tree().get_root_node())

        if not is_into or (sn_dst and not sn_dst.node.is_dir()):
            # cannot drop into a file; just use parent in this case
            sn_dst = self.con.cacheman.get_parent_sn_for_sn(sn_dst)

        if not sn_dst:
            logger.error(f'[{self.con.tree_id}] Cancelling drop: no parent node for dropped location!')
        elif self.con.tree_id == drag_data.src_treecon.tree_id and self._is_dropping_on_itself(sn_dst, drag_data.sn_list):
            logger.debug(f'[{self.con.tree_id}] Cancelling drop: nodes were dropped in same location in the tree')
        else:
            logger.debug(f'[{self.con.tree_id}]Dropping into dest: {sn_dst.spid}')
            # So far we only support COPY.
            # "Left tree" here is the source tree, and "right tree" is the dst tree:
            change_maker = ChangeMaker(app=self.con.parent_win.app, left_tree=drag_data.src_treecon.get_tree(), right_tree=self.con.get_tree())
            change_maker.copy_nodes_left_to_right(drag_data.sn_list, sn_dst, UserOpType.CP)
            # This should fire listeners which ultimately populate the tree:
            op_list: Iterable[UserOp] = change_maker.right_side.change_tree.get_ops()
            self.con.parent_win.app.cacheman.enqueue_op_list(op_list)

    def _check_drop(self):
        """Drag & Drop 4/4: Check UID of the dragged data against the UID of the dropped data.
        If they match, then we are the target."""
        if not self._drop_data or not self._drag_data or self._drop_data[0] != self._drag_data.dd_uid:
            return

        dd_uid, tree_path, is_into = self._drop_data

        self._do_drop(self.con.tree_id, self._drag_data, tree_path, is_into)

        # try to aid garbage collection
        self._drag_data = None
        self._drop_data = None

    def _is_dropping_on_itself(self, dst_sn: SPIDNodePair, sn_list: List[SPIDNodePair]):
        for sn in sn_list:
            logger.debug(f'[{self.con.tree_id}] DestNode="{dst_sn.spid}", DroppedNode="{sn.node}"')
            if dst_sn.node.is_parent_of(sn.node):
                return True
        return False

    def _on_root_path_updated(self, sender, new_root: SinglePathNodeIdentifier, err=None):
        logger.debug(f'[{self.con.tree_id}] Received signal: "{actions.ROOT_PATH_UPDATED}"')

        # Reload subtree and refresh display
        if not err and self.con.cacheman.reload_tree_on_root_path_update:
            logger.debug(f'[{self.con.tree_id}] Got new root. Reloading subtree for: {new_root}')
            # Loads from disk if necessary:
            self.con.reload(new_root, tree_display_mode=TreeDisplayMode.ONE_TREE_ALL_ITEMS, hide_checkboxes=True)
        else:
            # Just wipe out the old root and clear the tree
            self.con.set_tree(root=new_root)

    def _after_all_caches_loaded(self, sender):
        logger.debug(f'[{self.con.tree_id}] Received signal: "{actions.START_CACHEMAN_DONE}"; sending "{actions.LOAD_UI_TREE}" signal')
        dispatcher.send(signal=actions.LOAD_UI_TREE, sender=self.con.tree_id)

    # Remember, use member functions instead of lambdas, because PyDispatcher will remove refs
    def _on_set_status(self, sender, status_msg):
        GLib.idle_add(lambda: self.con.status_bar.set_label(status_msg))

    def _on_enable_ui_toggled(self, sender, enable):
        # Enable/disable listeners:
        self._ui_enabled = enable

    def _on_tree_selection_changed(self, selection):
        model, treeiter = selection.get_selected_rows()
        if treeiter is not None and len(treeiter) == 1:
            node = self.con.display_store.get_node_data(treeiter)
            if isinstance(node, LocalFileNode):
                logger.info(f'[{self.con.tree_id}] User selected node={node.node_identifier} md5="{node.md5}"')
            else:
                logger.info(f'[{self.con.tree_id}] User selected {self.con.display_store.get_node_name(treeiter)}')
        return self.on_selection_changed(treeiter)

    def _on_row_activated(self, tree_view, tree_path, col, tree_id):
        if not self._ui_enabled:
            logger.debug(f'[{self.con.tree_id}] Ignoring row activation - UI is disabled')
            # Allow it to propagate down the chain:
            return False
        selection = tree_view.get_selection()
        model, tree_paths = selection.get_selected_rows()
        if not tree_paths:
            logger.error(f'[{self.con.tree_id}] Row somehow activated with no selection!')
            return False
        else:
            logger.debug(f'[{self.con.tree_id}] User activated {len(tree_paths)} rows')

        # FIXME: GTK3's multiple item activation is terrible - find a way around it
        if len(tree_paths) == 1:
            if self.on_single_row_activated(tree_view=tree_view, tree_path=tree_path):
                return True
        else:
            if self.on_multiple_rows_activated(tree_view=tree_view, tree_paths=tree_paths):
                return True
        return False

    def _on_toggle_gtk_row_expanded_state(self, tree_view, parent_iter, parent_path, is_expanded):
        parent_data = self.con.display_store.get_node_data(parent_iter)
        logger.debug(f'[{self.con.tree_id}] Sending signal "{actions.NODE_EXPANSION_TOGGLED}" with is_expanded={is_expanded} for node: {parent_data}')
        if not parent_data.is_dir():
            raise RuntimeError(f'Node is not a directory: {type(parent_data)}; node_data')

        dispatcher.send(signal=actions.NODE_EXPANSION_TOGGLED, sender=self.con.tree_id, parent_iter=parent_iter, parent_path=parent_path,
                        node=parent_data, is_expanded=is_expanded, expand_all=False)

        return True

    def _on_key_press(self, tree_view, event, tree_id):
        """Fired when a key is pressed"""
        if not self._ui_enabled:
            logger.debug(f'[{self.con.tree_id}] Ignoring key press - UI is disabled')
            return False

        # Note: if the key sequence matches a Gnome keyboard shortcut, it will grab part
        # of the sequence and we will never get notified
        mods = []
        if (event.state & Gdk.ModifierType.CONTROL_MASK) == Gdk.ModifierType.CONTROL_MASK:
            mods.append('Ctrl')
        if (event.state & Gdk.ModifierType.SHIFT_MASK) == Gdk.ModifierType.SHIFT_MASK:
            mods.append('Shift')
        if (event.state & Gdk.ModifierType.META_MASK) == Gdk.ModifierType.META_MASK:
            mods.append('Meta')
        if (event.state & Gdk.ModifierType.SUPER_MASK) == Gdk.ModifierType.SUPER_MASK:
            mods.append('Super')
        if (event.state & Gdk.ModifierType.MOD1_MASK) == Gdk.ModifierType.MOD1_MASK:
            mods.append('Alt')
        logger.debug(f'[{self.con.tree_id}] Key pressed: {Gdk.keyval_name(event.keyval)} ({event.keyval}), mods: {" ".join(mods)}')

        if event.keyval == Gdk.KEY_Delete and self.con.treeview_meta.can_modify_tree:
            logger.debug(f'[{self.con.tree_id}]DELETE key detected!')
            if self.on_delete_key_pressed():
                return True
        return False

    def _on_tree_button_press(self, tree_view, event, tree_id):
        """Used for displaying context menu on right click"""
        if not self._ui_enabled:
            logger.debug(f'[{self.con.tree_id}] Ignoring button press - UI is disabled')
            return False

        if event.button == 3:  # right click
            path_at_pos = tree_view.get_path_at_pos(int(event.x), int(event.y))
            if not path_at_pos:
                logger.debug(f'[{self.con.tree_id}] Right-click but no node!')
                return False

            # tree_path, col, cell_x, cell_y = path_at_pos[0], path_at_pos[1], path_at_pos[2], path_at_pos[3]
            node_data = self.con.display_store.get_node_data(path_at_pos[0])
            logger.debug(f'[{self.con.tree_id}] User right-clicked on {node_data}')

            if self.on_row_right_clicked(event=event, tree_path=path_at_pos[0], node_data=node_data):
                # Suppress selection event:
                return True
        return False

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # LISTENERS end

    # ACTIONS begin
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    # To be optionally overridden:
    def on_selection_changed(self, treeiter):
        return False

    def on_single_row_activated(self, tree_view, tree_path):
        """Fired when an node is double-clicked or when an node is selected and Enter is pressed"""
        node: Node = self.con.display_store.get_node_data(tree_path)
        if node.is_dir():
            # Expand/collapse row:
            if tree_view.row_expanded(tree_path):
                tree_view.collapse_row(tree_path)
            else:
                tree_view.expand_row(path=tree_path, open_all=False)
            return True
        else:
            # Attempt to open it no matter where it is.
            # In the future, we should enhance this so that it will find the most convenient copy anywhere and open that

            op: Optional[UserOp] = self.con.app.cacheman.get_last_pending_op_for_node(node.uid)
            if op and not op.is_completed() and op.has_dst():
                logger.warning('TODO: test this!')

                if op.src_node.is_live():
                    _do_default_action_for_node(op.src_node, self.con.tree_id)
                    return True
                elif op.dst_node.is_live():
                    _do_default_action_for_node(op.dst_node, self.con.tree_id)
                    return True
            elif node.is_live():
                _do_default_action_for_node(node, self.con.tree_id)
                return True
            else:
                logger.debug(f'Aborting activation: file does not exist: {node}')
        return False

    def on_multiple_rows_activated(self, tree_view, tree_paths):
        """Fired when multiple items are selected and Enter is pressed"""
        if len(tree_paths) > 20:
            self.con.parent_win.show_error_msg(f'Too many items selected', f'You selected {len(tree_paths)} items, which is too many for you.\n\n'
                                               f'Try selecting less items first. This message exists for your protection. You child.')
        for tree_path in tree_paths:
            self.on_single_row_activated(tree_view, tree_path)
        return True

    def on_delete_key_pressed(self):
        if self.con.treeview_meta.can_modify_tree:
            selected_sn_list: List[SPIDNodePair] = self.con.display_store.get_multiple_selection_sn_list()
            if selected_sn_list:
                # TODO: change this to DELETE_SUBTREE.
                # TODO: refactor to send SN instead of node
                for selected_sn in selected_sn_list:
                    dispatcher.send(signal=actions.DELETE_SUBTREE, sender=self.con.tree_id, sn=selected_sn.node)
                return True
        return False

    def on_row_right_clicked(self, event, tree_path, node_data: Node):
        if node_data.is_ephemereal():
            logger.debug(f'[{self.con.tree_id}] User right-clicked on ephemereal node. Ignoring')
            return
        id_clicked = node_data.uid
        sel_items_tuple = self.con.display_store.get_multiple_selection_and_paths()
        selected_items: List[Node] = sel_items_tuple[0]
        selected_tree_paths: List[Gtk.TreePath] = sel_items_tuple[1]

        clicked_on_selection = False

        if len(selected_items) > 1:
            # Multiple selected items:
            for item in selected_items:
                if item.uid == id_clicked:
                    clicked_on_selection = True

        if clicked_on_selection:
            objs_type = _get_items_type(selected_items)

            # User right-clicked on selection -> apply context menu to all selected items:
            context_menu = self._context_menus_by_type[objs_type].build_context_menu_multiple(selected_items, selected_tree_paths)
            if context_menu:
                context_menu.popup_at_pointer(event)
                # Suppress selection event
                return True
            else:
                return False

        # FIXME: what about logical nodes?
        # Singular item, or singular selection (equivalent logic). Display context menu:
        context_menu = self._context_menus_by_type[node_data.node_identifier.tree_type].build_context_menu(tree_path, node_data)
        if context_menu:
            context_menu.popup_at_pointer(event)
            return True

        return False

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # ACTIONS end


def _get_items_type(selected_items: List):
    gdrive_count = 0
    fmeta_count = 0

    if len(selected_items) > 1:
        # Multiple selected items:
        for item in selected_items:
            if item.node_identifier.tree_type == TREE_TYPE_GDRIVE:
                gdrive_count += 1
            elif item.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
                fmeta_count += 1

    # determine object types
    if gdrive_count and fmeta_count:
        return TREE_TYPE_MIXED
    elif gdrive_count:
        return TREE_TYPE_GDRIVE
    else:
        return TREE_TYPE_LOCAL_DISK


def _do_default_action_for_node(node: Node, tree_id: str):
    if node.node_identifier.tree_type == TREE_TYPE_LOCAL_DISK:
        dispatcher.send(signal=actions.CALL_XDG_OPEN, sender=tree_id, full_path=node.get_single_path())
        return True
    elif node.node_identifier.tree_type == TREE_TYPE_GDRIVE:
        dispatcher.send(signal=actions.DOWNLOAD_FROM_GDRIVE, sender=tree_id, node=node)
        return True

