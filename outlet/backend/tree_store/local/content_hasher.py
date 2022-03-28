# -*- coding: utf-8 -*-
"""
@author: Sam Schott  (ss2151@cam.ac.uk)

(c) Sam Schott; This work is licensed under a Creative Commons
Attribution-NonCommercial-NoDerivs 2.0 UK: England & Wales License.

Note: this file was copied from the excellent Maestral Dropbox project

"""
import copy
import hashlib

# From: https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
import logging
import os
import pathlib
import subprocess
from typing import Optional, Tuple
from constants import IS_MACOS, MAX_FS_LINK_DEPTH, READ_CHUNK_SIZE, TreeType

logger = logging.getLogger(__name__)


def _compute_md5_in_python(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(READ_CHUNK_SIZE), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def compute_md5(filename):
    if IS_MACOS:
        try:
            output = subprocess.check_output(['/sbin/md5', '-q', filename]).decode().split('\n')[0]
        except (subprocess.CalledProcessError, RuntimeError) as err:
            logger.error(f'While computing MD5 via MacOS tool for file "{filename}": {repr(err)}')
            output = None
        if not output:
            logger.warning(f'Failed to compute MD5 using Mac cmdlkne tool; falling back to Python')
            return _compute_md5_in_python(filename)

        return output
    else:
        return _compute_md5_in_python(filename)


def compute_dropbox_hash(filename):
    hasher = DropboxContentHasher()
    with open(filename, 'rb') as f:
        while True:
            chunk = f.read(READ_CHUNK_SIZE)  # or whatever chunk size you want
            if len(chunk) == 0:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def calculate_signatures(full_path: str, staging_path: str = None) -> Tuple[Optional[str], Optional[str]]:
    try:
        # Open,close, read file and calculate hash of its contents
        if staging_path:
            md5: Optional[str] = compute_md5(staging_path)
        else:
            md5: Optional[str] = compute_md5(full_path)

        # TODO: compute SHA-256 at the same time. See: compute_dropbox_hash()
        sha256: Optional[str] = None

        return md5, sha256
    except FileNotFoundError:
        # Possibly a link instead:
        if os.path.islink(full_path):
            count_attempt = 0
            while count_attempt < MAX_FS_LINK_DEPTH:
                target = pathlib.Path(os.readlink(full_path)).resolve()
                if not target:
                    logger.warning(f'Broken link, skipping: "{full_path}" -> "{target}"')
                    return None, None
                logger.debug(f'Resolved link (depth {count_attempt}): "{full_path}" -> "{target}"')
                full_path = target
                try:
                    md5: Optional[str] = compute_md5(full_path)
                    # sha256 = local.content_hasher.dropbox_hash(full_path)
                    sha256: Optional[str] = None
                    return md5, sha256
                except FileNotFoundError:
                    count_attempt += 1

            # exceeded max attempts
            logger.error(f'Max link depth ({MAX_FS_LINK_DEPTH}) exceeded: "{full_path}"')
        else:
            # Can happen often if temp files are rapidly created/destroyed. Assume it will be cleaned up elsewhere
            logger.debug(f'Could not calculate signature: file not found; skipping: {full_path}')
        # Return None. Will be assumed to be a deleted file
        return None, None


class DropboxContentHasher(object):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS DropboxContentHasher

    Computes a hash using the same algorithm that the Dropbox API uses for the
    the "content_hash" metadata field.
    The digest() method returns a raw binary representation of the hash.  The
    hexdigest() convenience method returns a hexadecimal-encoded version, which
    is what the "content_hash" metadata field uses.
    This class has the same interface as the hashers in the standard 'hashlib'
    package.
    Example:
        hasher = DropboxContentHasher()
        with open('some-file', 'rb') as f:
            while True:
                chunk = f.read(1024)  # or whatever chunk size you want
                if len(chunk) == 0:
                    break
                hasher.update(chunk)
        print(hasher.hexdigest())
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    BLOCK_SIZE = 4 * 1024 * 1024

    def __init__(self):
        self._overall_hasher = hashlib.sha256()
        self._block_hasher = hashlib.sha256()
        self._block_pos = 0

        self.digest_size = self._overall_hasher.digest_size

    def update(self, new_data):
        if self._overall_hasher is None:
            raise AssertionError(
                "can't use this object anymore; you already called digest()")

        assert isinstance(new_data, bytes), (
            "Expecting a byte string, got {!r}".format(new_data))

        new_data_pos = 0
        while new_data_pos < len(new_data):
            if self._block_pos == self.BLOCK_SIZE:
                self._overall_hasher.update(self._block_hasher.digest())
                self._block_hasher = hashlib.sha256()
                self._block_pos = 0

            space_in_block = self.BLOCK_SIZE - self._block_pos
            part = new_data[new_data_pos:(new_data_pos + space_in_block)]
            self._block_hasher.update(part)

            self._block_pos += len(part)
            new_data_pos += len(part)

    def _finish(self):
        if self._overall_hasher is None:
            raise AssertionError("can't use this object anymore; you already called digest() or hexdigest()")

        if self._block_pos > 0:
            self._overall_hasher.update(self._block_hasher.digest())
            self._block_hasher = None
        h = self._overall_hasher
        self._overall_hasher = None  # Make sure we can't use this object anymore.
        return h

    def digest(self):
        return self._finish().digest()

    def hexdigest(self):
        return self._finish().hexdigest()

    def copy(self):
        c = DropboxContentHasher.__new__(DropboxContentHasher)
        c._overall_hasher = self._overall_hasher.copy()
        c._block_hasher = self._block_hasher.copy()
        c._block_pos = self._block_pos
        return c


class StreamHasher(object):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS StreamHasher

    A wrapper around a file-like object (either for reading or writing)
    that hashes everything that passes through it.  Can be used with
    DropboxContentHasher or any 'hashlib' hasher.
    Example:
        hasher = DropboxContentHasher()
        with open('some-file', 'rb') as f:
            wrapped_f = StreamHasher(f, hasher)
            response = some_api_client.upload(wrapped_f)
        locally_computed = hasher.hexdigest()
        assert response.content_hash == locally_computed
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, f, hasher):
        self._f = f
        self._hasher = hasher

    def close(self):
        return self._f.close()

    def flush(self):
        return self._f.flush()

    def fileno(self):
        return self._f.fileno()

    def tell(self):
        return self._f.tell()

    def read(self, *args):
        b = self._f.read(*args)
        self._hasher.update(b)
        return b

    def write(self, b):
        self._hasher.update(b)
        return self._f.write(b)

    def next(self):
        b = self._f.next()
        self._hasher.update(b)
        return b

    def readline(self, *args):
        b = self._f.readline(*args)
        self._hasher.update(b)
        return b

    def readlines(self, *args):
        bs = self._f.readlines(*args)
        for b in bs:
            self._hasher.update(b)
        return b
