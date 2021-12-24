import errno
import logging
import os
import shutil
from typing import Optional

from backend.tree_store.local import content_hasher
from error import IdenticalFileExistsError
from logging_constants import SUPER_DEBUG_ENABLED
from model.node.local_disk_node import LocalFileNode, LocalNode
from util import file_util

logger = logging.getLogger(__name__)


class LocalFileUtil:
    def __init__(self, cacheman):
        self.cacheman = cacheman

    def ensure_up_to_date(self, node: LocalFileNode) -> LocalFileNode:
        """Returns either a LocalFileNode with a signature, or raises an error."""

        # First, make sure node has a signature:
        if not node.has_signature():
            # This can happen if the node was just added but lazy sig scan hasn't gotten to it yet. Just compute it ourselves here
            node_with_signatures: LocalFileNode = content_hasher.try_calculating_signatures(node)
            if not node_with_signatures:
                raise RuntimeError(f'Failed to calculate signature for node: {node}')
            return node_with_signatures
        assert node.has_signature()

        # Now, build a new node from scratch to ensure its meta (e.g. modify_ts) is up-to-date.
        # We'll take a shortcut and assume that if the meta matches what we had, the signature hasn't changed either
        fresh_node: LocalFileNode = self.cacheman.build_local_file_node(full_path=node.get_single_path(), must_scan_signature=False, is_live=True)
        if not fresh_node:
            raise RuntimeError(f'File missing: {node.get_single_path()}')
        fresh_node.copy_signature_if_meta_matches(node)
        if fresh_node.has_signature():
            return fresh_node

        # Otherwise: meta was out-of-date: we do not like this.
        node_with_signatures: Optional[LocalNode] = content_hasher.try_calculating_signatures(fresh_node)
        if not node_with_signatures:
            raise RuntimeError(f'File has unexpectedly changed, and failed to calculate its new signature: {node.node_identifier}')

        # Was signature also out-of-date?
        if not node_with_signatures.is_signature_match(node):
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
            existing_dst_node = content_hasher.try_calculating_signatures(dst_node)
            if existing_dst_node and existing_dst_node.is_signature_match(src_node):
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
            # TODO: custom exception class
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), dst_path)

        dst_node = content_hasher.try_calculating_signatures(dst_node)
        if not dst_node:
            raise RuntimeError(f'Failed to calculate signature for: {dst_path}')
        if dst_node.is_signature_match(src_node):
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
            if not staging_node.is_signature_match(src_node):
                raise RuntimeError(f'Signature of copied file does not match: src_path="{src_path}", '
                                   f'src_md5={src_node.md5}, staging_file="{staging_path}", staging_md5={staging_node.md5}')
    
    def copy_meta(self, src_node: LocalFileNode, dst_path: str):
        try:
            if SUPER_DEBUG_ENABLED:
                logger.debug(f'Copying stats from {src_node.node_identifier} to "{dst_path}"')

            # Copy the permission bits, last access time, last modification time, and flags:
            shutil.copystat(src_node.get_single_path(), dst=dst_path, follow_symlinks=False)

            # FIXME: set creation time
            # from subprocess import call
            # command = 'SetFile -d ' + '"05/06/2019 "' + '00:00:00 ' + complete_path
            # call(command, shell=True)

            # OR:
            # os.system('SetFile -d "{}" {}'.format(date.strftime('%m/%d/%Y %H:%M:%S'), filePath))
        except Exception:
            logger.error(f'Exception while copying file meta (src: "{src_node.get_single_path()}" dst: "{dst_path}"')
            raise
    
        dst_node: LocalFileNode = self.cacheman.build_local_file_node(full_path=dst_path, must_scan_signature=False, is_live=True)
        if not dst_node:
            raise RuntimeError(f'Failed to build fresh node after copying meta for path: {dst_path}')
        if not dst_node.meta_matches(src_node):
            raise RuntimeError(f'Dst node meta does not match src node! src={src_node} dst={dst_node}')
