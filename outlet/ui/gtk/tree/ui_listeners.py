import logging
from typing import List, Optional, Set

import gi
from pydispatch import dispatcher

from backend.diff.change_maker import SPIDNodePair
from constants import DirConflictPolicy, DragOperation, FileConflictPolicy, TreeID, TreeType
from model.display_tree.display_tree import DisplayTree
from model.node.node import Node
from model.node_identifier import GUID
from model.uid import UID
from model.user_op import UserOp
from signal_constants import Signal
from ui.gtk.tree.context_menu import TreeContextMenu
from util.has_lifecycle import HasLifecycle

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gdk, Gtk

logger = logging.getLogger(__name__)


class DragAndDropData:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DragAndDropData
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, dd_uid: UID, src_treecon, sn_list: List[SPIDNodePair], drag_op: DragOperation,
                 dir_conflict_policy: DirConflictPolicy, file_conflict_policy: FileConflictPolicy):
        self.dd_uid: UID = dd_uid
        self.src_treecon = src_treecon
        self.sn_list: List[SPIDNodePair] = sn_list
        self.drag_op: DragOperation = drag_op
        self.dir_conflict_policy: DirConflictPolicy = dir_conflict_policy
        self.file_conflict_policy: FileConflictPolicy = file_conflict_policy


class TreeUiListeners(HasLifecycle):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS TreeUiListeners
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, controller):
        HasLifecycle.__init__(self)
        self.con = controller
        self._ui_enabled = True
        self._drag_data: Optional[DragAndDropData] = None
        self._drop_data = None
        self._connected_treeview_eids = []
        self._connected_selection_eid = None
        self._context_menu = TreeContextMenu(self.con)

    def start(self):
        logger.debug(f'[{self.con.tree_id}] TreeUiListeners start')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=Signal.TOGGLE_UI_ENABLEMENT, receiver=self._on_enable_ui_toggled)

        self.connect_dispatch_listener(signal=Signal.DISPLAY_TREE_CHANGED, receiver=self._on_display_tree_changed_checkroot)

        if self.con.treeview_meta.can_modify_tree:
            self.connect_dispatch_listener(signal=Signal.DRAG_AND_DROP, receiver=self._receive_drag_data_signal)
            self.connect_dispatch_listener(signal=Signal.DRAG_AND_DROP_DIRECT, receiver=self._do_drop)
            # ^^^ mostly for testing

        self.connect_gtk_listeners()

    def shutdown(self):
        HasLifecycle.shutdown(self)
        self.disconnect_gtk_listeners()

    def connect_gtk_listeners(self):
        # TreeView
        # double-click or enter key:
        eid = self.con.tree_view.connect("row-activated", self._on_row_activated, self.con.tree_id)
        self._connected_treeview_eids.append(eid)
        # right-click:
        eid = self.con.tree_view.connect('button-press-event', self._on_tree_button_press, self.con.tree_id)
        self._connected_treeview_eids.append(eid)
        # other keys like 'Del'
        eid = self.con.tree_view.connect('key-press-event', self._on_key_press, self.con.tree_id)
        self._connected_treeview_eids.append(eid)
        # user clicked on the expand
        eid = self.con.tree_view.connect('row-expanded', self._on_toggle_gtk_row_expanded_state, True)
        self._connected_treeview_eids.append(eid)
        eid = self.con.tree_view.connect('row-collapsed', self._on_toggle_gtk_row_expanded_state, False)
        self._connected_treeview_eids.append(eid)
        self._connected_selection_eid = self.con.tree_view.get_selection().connect("changed", self._on_tree_selection_changed)

        if self.con.treeview_meta.can_modify_tree:
            action_mask = Gdk.DragAction.DEFAULT | Gdk.DragAction.MOVE | Gdk.DragAction.COPY
            self.con.tree_view.enable_model_drag_source(Gdk.ModifierType.BUTTON1_MASK, [], action_mask)
            self.con.tree_view.enable_model_drag_dest([], action_mask)
            # Text targets:
            self.con.tree_view.drag_dest_set_target_list(None)
            self.con.tree_view.drag_source_set_target_list(None)
            self.con.tree_view.drag_dest_add_text_targets()
            self.con.tree_view.drag_source_add_text_targets()
            eid = self.con.tree_view.connect("drag-data-received", self._drag_data_received)
            self._connected_treeview_eids.append(eid)
            eid = self.con.tree_view.connect("drag-data-get", self._drag_data_get)
            self._connected_treeview_eids.append(eid)
            # FIXME Want to remove highlight when dropping in non-dir rows. But this is not the correct way to do this.
            # self.con.tree_view.connect('drag-motion', self._on_drag_motion)

    def disconnect_gtk_listeners(self):
        for eid in self._connected_treeview_eids:
            self.con.tree_view.disconnect(eid)
        self._connected_treeview_eids.clear()

        if self._connected_selection_eid:
            self.con.tree_view.get_selection().disconnect(self._connected_selection_eid)
            self._connected_selection_eid = None

    # LISTENERS begin
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

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
        selected_sn_list: List[SPIDNodePair] = self.con.display_store.get_multiple_selection()
        if selected_sn_list:
            # Avoid complicated, undocumented GTK3 garbage by just sending a UID along with needed data via the dispatcher. See _check_drop()
            dd_uid = self.con.app.ui_uid_generator.next_uid()
            action = drag_context.get_selected_action()
            # FIXME: add support for more operations and conflict policies!
            drag_data = DragAndDropData(dd_uid, self.con, selected_sn_list,
                                        DragOperation.COPY, DirConflictPolicy.MERGE, FileConflictPolicy.REPLACE_IF_NEWER_VER)
            dispatcher.send(signal=Signal.DRAG_AND_DROP, sender=self.con.tree_id, data=drag_data)
            selection_data.set_text(str(dd_uid), -1)
        else:
            selection_data.set_text('', -1)

    def _receive_drag_data_signal(self, sender, data: DragAndDropData):
        """Drag & Drop 2 or 3 /4: receive drag data at dest"""
        logger.debug(f'[{self.con.tree_id}] Received signal: "{Signal.DRAG_AND_DROP.name}"')
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
        if sender != self.con.tree_id:
            return

        # Puts the drag data into/adjacent to the given tree_path.
        logger.info(f'[{self.con.tree_id}] We received a drop of {len(drag_data.sn_list)} nodes!')

        if tree_path:
            sn_dst: SPIDNodePair = self.con.display_store.get_node_data(tree_path)
        else:
            # Assume we are dropping into the tree root
            is_into = True
            sn_dst = SPIDNodePair(self.con.get_tree().root_identifier, self.con.get_tree().get_root_node())

        src_guid_list = [sn.spid.guid for sn in drag_data.sn_list]
        self.con.app.backend.drop_dragged_nodes(src_tree_id=drag_data.src_treecon.tree_id, src_guid_list=src_guid_list, is_into=is_into,
                                                dst_tree_id=self.con.tree_id, dst_guid=sn_dst.spid.guid, drag_operation=drag_data.drag_op,
                                                dir_conflict_policy=drag_data.dir_conflict_policy,
                                                file_conflict_policy=drag_data.file_conflict_policy)

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

    def _on_display_tree_changed_checkroot(self, sender, tree: DisplayTree):
        if sender != self.con.tree_id:
            return

        logger.debug(f'[{self.con.tree_id}] Received signal: "{Signal.DISPLAY_TREE_CHANGED.name}"')

        # Reload subtree and refresh display
        if tree.is_root_exists():
            logger.debug(f'[{self.con.tree_id}] Tree root exists. Reloading subtree for: {tree.get_root_spid()}')
            # Loads from disk if necessary:
            self.con.reload(tree)
        else:
            # Just wipe out the old root and clear the tree
            self.con.set_tree(tree)

    def _on_enable_ui_toggled(self, sender, enable):
        # Enable/disable listeners:
        self._ui_enabled = enable

    def _on_tree_selection_changed(self, selection):
        model, treeiter = selection.get_selected_rows()
        selected_sn_list: [SPIDNodePair] = []
        if treeiter is not None:
            if len(treeiter) == 1:
                sn = self.con.display_store.get_node_data(treeiter)
                if sn.node and sn.node.md5:
                    md5 = f' md5="{sn.node.md5}'
                else:
                    md5 = ''
                logger.debug(f'[{self.con.tree_id}] User selected node={sn.spid} md5={md5}"')

                selected_sn_list.append(sn)
            else:
                logger.debug(f'[{self.con.tree_id}] User selected {len(treeiter)} nodes')
                for i in treeiter:
                    sn = self.con.display_store.get_node_data(i)
                    selected_sn_list.append(sn)

            def report_tree_selection():
                self._report_tree_selection(selected_sn_list)

            # Do this async so that there's no chance of blocking the user:
            # TODO: put this in a HoldoffTimer so it doesn't execute in rapid-fire
            dispatcher.send(signal=Signal.ENQUEUE_UI_TASK, sender=self.con.tree_id, task_func=report_tree_selection)

            dispatcher.send(signal=Signal.TREE_SELECTION_CHANGED, sender=self.con.tree_id, sn_list=selected_sn_list)
        return False

    def _report_tree_selection(self, selected_sn_list: [SPIDNodePair]):
        selected: Set[GUID] = set()
        for sn in selected_sn_list:
            selected.add(sn.spid.guid)

        # Report to the backend
        self.con.backend.set_selected_rows(tree_id=self.con.tree_id, selected=selected)

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
        sn = self.con.display_store.get_node_data(parent_iter)
        logger.debug(f'[{self.con.tree_id}] Sending signal "{Signal.NODE_EXPANSION_TOGGLED.name}" with is_expanded={is_expanded}'
                     f' for node: {sn.spid}')
        if not sn.node.is_dir():
            raise RuntimeError(f'Node is not a directory: {type(sn.node)}; node_data')

        dispatcher.send(signal=Signal.NODE_EXPANSION_TOGGLED, sender=self.con.tree_id, parent_iter=parent_iter, parent_path=parent_path,
                        sn=sn, is_expanded=is_expanded, expand_all=False)

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
            clicked_sn = self.con.display_store.get_node_data(path_at_pos[0])
            logger.debug(f'[{self.con.tree_id}] User right-clicked on {clicked_sn}')

            if self.on_row_right_clicked(event=event, tree_path=path_at_pos[0], clicked_sn=clicked_sn):
                # Suppress selection event:
                return True
        return False

    # ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲
    # LISTENERS end

    # ACTIONS begin
    # ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼

    def on_single_row_activated(self, tree_view, tree_path):
        """Fired when an node is double-clicked or when an node is selected and Enter is pressed"""
        sn: SPIDNodePair = self.con.display_store.get_node_data(tree_path)
        if sn.node.is_dir():
            # Expand/collapse row:
            if tree_view.row_expanded(tree_path):
                tree_view.collapse_row(tree_path)
            else:
                tree_view.expand_row(path=tree_path, open_all=False)
            return True
        else:
            # Attempt to open it no matter where it is.
            # In the future, we should enhance this so that it will find the most convenient copy anywhere and open that

            op: Optional[UserOp] = self.con.app.backend.get_last_pending_op(sn.node.uid)
            if op and op.has_dst():
                logger.warning('TODO: test this!')

                if op.src_node.is_live():
                    _do_default_action_for_node(op.src_node, self.con.tree_id)
                    return True
                elif op.dst_node.is_live():
                    _do_default_action_for_node(op.dst_node, self.con.tree_id)
                    return True
            elif sn.node.is_live():
                _do_default_action_for_node(sn.node, self.con.tree_id)
                return True
            else:
                logger.debug(f'Aborting activation: file does not exist: {sn.node}')
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
        if not self.con.treeview_meta.can_modify_tree:
            return False

        selected_node_list: List[Node] = self.con.display_store.get_multiple_selection()
        if selected_node_list:
            if self.con.parent_win.show_question_dialog('Confirm Delete', f'Are you sure you want delete these {len(selected_node_list)} items?'):
                dispatcher.send(signal=Signal.DELETE_SUBTREE, sender=self.con.tree_id, node_list=selected_node_list)
                return True

        return False

    def on_row_right_clicked(self, event, tree_path, clicked_sn: SPIDNodePair):
        if clicked_sn.node.is_ephemereal():
            logger.debug(f'[{self.con.tree_id}] User right-clicked on ephemereal node. Ignoring')
            return
        guid_clicked = clicked_sn.spid.guid
        sel_items_tuple = self.con.display_store.get_multiple_selection_and_paths()
        selected_sn_list: List[SPIDNodePair] = sel_items_tuple[0]
        selected_tree_paths: List[Gtk.TreePath] = sel_items_tuple[1]

        clicked_on_selection = False

        if len(selected_sn_list) > 1:
            # Multiple selected items:
            for selected_sn in selected_sn_list:
                if selected_sn.spid.guid == guid_clicked:
                    clicked_on_selection = True

        if clicked_on_selection:
            # User right-clicked on selection -> apply context menu to all selected items:
            sn_list = selected_sn_list
            tree_path_list = selected_tree_paths
        else:
            # Singular item, or singular selection (equivalent logic). Display context menu:
            sn_list = [clicked_sn]
            tree_path_list = [tree_path]

        guid_list = [sn.spid.guid for sn in sn_list]
        menu_item_list = self.con.backend.get_context_menu(tree_id=self.con.tree_id, identifier_list=guid_list)
        context_menu = self._context_menu.build_context_menu(menu_item_list, sn_list, tree_path_list)

        if context_menu:
            context_menu.popup_at_pointer(event)
            # Suppress selection event
            return True
        else:
            return False

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # ACTIONS end


def _do_default_action_for_node(node: Node, tree_id: TreeID):
    if node.tree_type == TreeType.LOCAL_DISK:
        # FIXME: this will not work for non-local files
        dispatcher.send(signal=Signal.CALL_XDG_OPEN, sender=tree_id, full_path=node.node_identifier.get_single_path())
        return True
    elif node.tree_type == TreeType.GDRIVE:
        dispatcher.send(signal=Signal.DOWNLOAD_FROM_GDRIVE, sender=tree_id, node=node)
        return True
