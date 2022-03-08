import copy
import errno
import logging
import os
import platform
import shutil
import subprocess
from datetime import datetime
from typing import Optional

from backend.tree_store.local import content_hasher
from constants import IS_MACOS, IS_WINDOWS, MACOS_SETFILE_DATETIME_FMT
from error import IdenticalFileExistsError
from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED
from model.node.local_disk_node import LocalFileNode, LocalNode
from model.node.node import Node
from util import file_util, time_util

logger = logging.getLogger(__name__)


class LocalFileUtil:
    def __init__(self, cacheman):
        self.cacheman = cacheman

    def try_calculating_signature(self, node: LocalFileNode) -> LocalFileNode:
        content_meta = self.cacheman.calculate_signature_for_local_file(node.device_uid, node.get_single_path())
        if not content_meta:
            raise RuntimeError(f'Failed to calculate signature for node: {node}')

        node_with_signatures: LocalFileNode = copy.deepcopy(node)
        node_with_signatures.content_meta = content_meta

        return node_with_signatures

    def ensure_up_to_date(self, node: LocalFileNode) -> LocalFileNode:
        """Returns either a LocalFileNode with a signature, or raises an error."""

        # First, make sure node has a signature:
        if not node.has_signature():
            # This can happen if the node was just added but lazy sig scan hasn't gotten to it yet. Just compute it ourselves here
            return self.try_calculating_signature(node)
        assert node.has_signature()

        # Now, build a new node from scratch to ensure its meta (e.g. modify_ts) is up-to-date.
        # We'll take a shortcut and assume that if the meta matches what we had, the signature hasn't changed either
        fresh_node: LocalFileNode = self.cacheman.build_local_file_node(full_path=node.get_single_path(), must_scan_signature=False, is_live=True)
        if not fresh_node:
            raise RuntimeError(f'File missing: {node.get_single_path()}')
        fresh_node.copy_signature_if_is_meta_equal(node)
        if fresh_node.has_signature():
            return fresh_node

        # Otherwise: meta was out-of-date: we do not like this.
        node_with_signatures: Optional[LocalNode] = self.try_calculating_signature(fresh_node)
        if not node_with_signatures:
            raise RuntimeError(f'File has unexpectedly changed, and failed to calculate its new signature: {node.node_identifier}')

        # Was signature also out-of-date?
        if not node_with_signatures.is_signature_equal(node):
            raise RuntimeError(f'File has unexpectedly changed: {node.node_identifier}; expected: {node}, found: {node_with_signatures}')
        else:
            # Signature is the same but other meta changed
            # TODO: maybe allow this?
            raise RuntimeError(f'File meta has unexpectedly changed: {node.node_identifier}; expected: {node}, found: {node_with_signatures}')

    def copy_file_new(self, src_node: LocalFileNode, dst_node: LocalFileNode, staging_path: str, verify: bool, copy_meta_also: bool):
        """Copies the src (src_path) to the destination path (dst_path), by first doing the copy to an
        intermediary location (staging_path) and then moving it to the destination once its signature
        has been verified.

        Raises an error if a file is already present at the destination."""
        assert not dst_node.is_live(), f'Should not be live: {dst_node}'
        dst_path = dst_node.get_single_path()
        # dst node actually exists? (this implies our cached copy is not accurate):
        if os.path.exists(dst_path):
            existing_dst_node = self.try_calculating_signature(dst_node)
            if existing_dst_node and existing_dst_node.is_signature_equal(src_node):
                msg = f'File with identical content already exists at dst: {dst_path}'
                logger.info(msg)

                if copy_meta_also:
                    # If this returns false, meta is already the same
                    if not self.copy_meta(src_node, dst_path):
                        # This will be caught and treated as a no-op
                        raise IdenticalFileExistsError(msg)
                else:
                    # This will be caught and treated as a no-op
                    raise IdenticalFileExistsError(msg)
            else:
                # signature mismatch
                msg = f'Found unexpected file at dst ("{dst_path}"): {existing_dst_node}'
                logger.debug(f'Throwing FileExistsError: {msg}')
                raise FileExistsError(msg)

        self.copy_to_staging(src_node, staging_path, verify)

        file_util.move_to_dst(staging_path, dst_path, replace=False)

        if copy_meta_also:
            self.copy_meta(src_node, dst_path)

    def copy_file_update(self, src_node: LocalFileNode, dst_node: LocalFileNode, staging_path: str,
                         verify: bool, update_meta_also: bool):
        """Copies the src (src_path) to the destination path (dst_path) via a staging dir, but first
        verifying that a file already exists there and it has the expected MD5; failing otherwise"""
    
        dst_path = dst_node.get_single_path()
        if not os.path.exists(dst_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), dst_path)

        dst_node = self.try_calculating_signature(dst_node)
        if not dst_node:
            raise RuntimeError(f'Failed to calculate signature for: {dst_path}')
        if dst_node.is_signature_equal(src_node):
            msg = f'Identical file already exists at dst: {dst_path}'
            logger.info(msg)
    
            if update_meta_also:
                if not self.copy_meta(src_node, dst_path):
                    # This will be caught and treated as a no-op
                    raise IdenticalFileExistsError(msg)
            else:
                # This will be caught and treated as a no-op
                raise IdenticalFileExistsError(msg)
        else:
            msg = f'File to overwrite ("{dst_path}") has unexpected signature: {dst_node.md5} (expected: {src_node.md5})'
            logger.debug(f'Throwing FileExistsError: {msg}')
            raise FileExistsError(msg)

        self.copy_to_staging(src_node, staging_path, verify)
    
        file_util.move_to_dst(staging_path, dst_path, replace=True)
    
        if update_meta_also:
            self.copy_meta(src_node, dst_path)

    def copy_to_staging(self, src_node: LocalFileNode, staging_path, verify: bool):
        # (Staging) make parent directories if not exist
        staging_parent, staging_file = os.path.split(staging_path)
        try:
            os.makedirs(name=staging_parent, exist_ok=True)
        except Exception as err:
            logger.error(f'Exception while making staging dir: {staging_parent}')
            raise
    
        src_path: str = src_node.get_single_path()
        try:
            shutil.copyfile(src_path, dst=staging_path, follow_symlinks=False)
        except Exception as err:
            logger.error(f'Exception while copying file to staging: {src_path}')
            raise
    
        if verify:
            staging_node: LocalFileNode = self.cacheman.build_local_file_node(full_path=staging_path, must_scan_signature=True, is_live=True)
            if not staging_node:
                raise RuntimeError(f'Failed to calculate signature for staging file: "{staging_path}"')
            if not staging_node.is_signature_equal(src_node):
                raise RuntimeError(f'Signature of copied file does not match: src_path="{src_path}", '
                                   f'src_md5={src_node.md5}, staging_file="{staging_path}", staging_md5={staging_node.md5}')
    
    def copy_meta(self, src_node: Node, dst_path: str) -> LocalNode:
        """Sets create_ts, modify_ts (and access_ts) for dst_path, using the values found in src_node.
        Note that src_node does not need to be a LocalNode."""
        try:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Copying stats from {src_node.node_identifier} to "{dst_path}"')

            if IS_WINDOWS:
                if TRACE_ENABLED:
                    logger.debug(f'OS is Windows; no need for special handling of creation time')
            elif IS_MACOS:
                """
                MacOS:
                The command for setting creation time via the command line is for example:
                    SetFile -d "05/06/2019 00:00:00" path/to/myfile

                HOWEVER:
                The above command only guarantees second precision! We'll exploit a feature of the operating system to get milliseconds.
                If we set a modification time (which has millis) which is earlier than the existing creation time, the OS will set its creation
                time identically.
                """
                if SUPER_DEBUG_ENABLED:
                    logger.debug(f'(MacOS): Setting creation date to now()')
                now_ts = time_util.now_ms()
                _macos_set_file_create_ts(dst_path, now_ts)
                create_ts_python = src_node.create_ts / 1000
                if TRACE_ENABLED:
                    logger.debug(f'(MacOS): Setting modify time to now: "{dst_path}" = {create_ts_python}')
                os.utime(dst_path, (create_ts_python, create_ts_python))

                dst_stat = os.stat(dst_path)
                create_ts = int(dst_stat.st_birthtime * 1000)
                if create_ts == src_node.create_ts:
                    logger.debug(f'Creation time already matches: "{dst_path}" = {create_ts}')
                else:
                    logger.error(f'Creation time incorrect: "{dst_path}" = {create_ts} (should be: {src_node.create_ts})')
            else:
                # FIXME: Set Linux create_ts
                logger.error(f'Possible meta loss! Setting local node creation time is not yet implemented on {platform.system().lower()}')

            # Set modify_ts (also set access_ts to same)
            modify_ts_python = src_node.modify_ts / 1000
            os.utime(dst_path, (modify_ts_python, modify_ts_python))

            # Copy the permission bits, last access time, last modification time, and flags. Note that this won't change ctime:
            # shutil.copystat(src_node.get_single_path(), dst=dst_path, follow_symlinks=False)

        except Exception:
            logger.error(f'Exception while copying file meta (src: "{src_node.node_identifier}" dst: "{dst_path}"')
            raise
    
        dst_node: LocalFileNode = self.cacheman.build_local_file_node(full_path=dst_path, must_scan_signature=False, is_live=True)
        if not dst_node:
            raise RuntimeError(f'Failed to build fresh node after copying meta for path: {dst_path}')
        if not dst_node.is_meta_equal(src_node):
            raise RuntimeError(f'Dst node meta does not match src node! src={src_node} dst={dst_node}')

        return dst_node


def _macos_set_file_create_ts(dst_path: str, create_ts: int):
    create_ts_datetime = datetime.fromtimestamp(create_ts / 1000)
    create_ts_formatted = create_ts_datetime.strftime(MACOS_SETFILE_DATETIME_FMT)
    command = ['SetFile', '-d', f'{create_ts_formatted}', dst_path]
    if SUPER_DEBUG_ENABLED:
        logger.debug(f'(MacOS): Setting creation date with cmd_line: {command}')
    rc = subprocess.call(command, shell=False)
    if rc != 0:
        raise RuntimeError(f'Failed to set the creation time ("SetFile" returned code={rc}) of "{dst_path}"')
    if SUPER_DEBUG_ENABLED:
        logger.debug(f'(MacOS): SetFile returned code={rc}')
