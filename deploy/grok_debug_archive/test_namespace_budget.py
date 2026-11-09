"""The admission budget must count the whole namespace without materializing it."""
import os
from pathlib import Path
import socket
import stat
import tempfile
import tracemalloc
import unittest
from unittest import mock

import nas_archive as nas


class NamespaceBudget(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def size(self):
        return nas.namespace_size(self.root, lambda: None)

    def test_counts_logical_bytes_in_every_directory_and_hardlink_path(self):
        total = 0
        for name in ["a", "b", ".run", "captures/nested"]:
            directory = self.root / name
            directory.mkdir(parents=True)
            for filename, size in [("body.gz", 17), (".failed.pending", 23)]:
                (directory / filename).write_bytes(b"x" * size)
                total += size
        os.link(self.root / "a/body.gz", self.root / "b/linked.gz")
        total += 17
        with (self.root / "sparse").open("wb") as output:
            output.truncate(1024 ** 2)
        self.assertEqual(self.size(), total + 1024 ** 2)

    def test_socket_is_ignored(self):
        endpoint = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(endpoint.close)
        endpoint.bind(str(self.root / "socket"))
        self.assertEqual(self.size(), 0)

    def test_file_directory_and_broken_symlinks_are_rejected(self):
        for kind in ["file", "directory", "broken"]:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as outside:
                target = Path(outside) / "target"
                if kind == "file":
                    target.write_bytes(b"private")
                elif kind == "directory":
                    target.mkdir()
                    (target / "body").write_bytes(b"private")
                link = self.root / "link"
                link.symlink_to(target)
                try:
                    code = "symlink_in_namespace" if kind == "directory" else "unsafe_namespace_object"
                    with self.assertRaisesRegex(nas.ArchiveError, code):
                        self.size()
                finally:
                    link.unlink()

    def test_fifo_is_rejected_without_opening_it(self):
        os.mkfifo(self.root / "fifo")
        with self.assertRaisesRegex(nas.ArchiveError, "unsafe_namespace_object"):
            self.size()

    def test_deadline_is_checked_within_a_flat_directory(self):
        for number in range(100):
            (self.root / str(number)).touch()
        calls = 0

        def deadline():
            nonlocal calls
            calls += 1
            if calls == 5:
                raise nas.LimitReached("run_time_limit")

        with self.assertRaisesRegex(nas.LimitReached, "run_time_limit"):
            nas.namespace_size(self.root, deadline)
        self.assertEqual(calls, 5)

    def test_scan_error_cannot_return_an_incomplete_budget(self):
        with mock.patch.object(nas.os, "scandir", side_effect=PermissionError("synthetic")):
            with self.assertRaises(PermissionError):
                self.size()

    def test_deadline_closes_all_directory_descriptors(self):
        (self.root / "a/b/c").mkdir(parents=True)
        (self.root / "a/b/c/body").write_bytes(b"data")
        opened = []
        original_open = os.open
        calls = 0

        def open_directory(*args, **kwargs):
            fd = original_open(*args, **kwargs)
            opened.append(fd)
            return fd

        def deadline():
            nonlocal calls
            calls += 1
            if calls == 5:
                raise nas.LimitReached("run_time_limit")

        with mock.patch.object(nas.os, "open", side_effect=open_directory):
            with self.assertRaises(nas.LimitReached):
                nas.namespace_size(self.root, deadline)
        # The deadline may interrupt either traversal worker. Every descriptor
        # actually opened must close, regardless of which worker reaches it first.
        self.assertGreaterEqual(len(opened), 2)
        self.assertLessEqual(len(opened), 4)
        for fd in opened:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_directory_swapped_to_symlink_is_not_traversed(self):
        (self.root / "child").mkdir()
        original_open = os.open
        with tempfile.TemporaryDirectory() as outside:
            (Path(outside) / "private").write_bytes(b"not in namespace")

            def open_directory(name, *args, **kwargs):
                if name == "child":
                    (self.root / "child").rmdir()
                    (self.root / "child").symlink_to(outside)
                return original_open(name, *args, **kwargs)

            with mock.patch.object(nas.os, "open", side_effect=open_directory):
                with self.assertRaises(OSError):
                    self.size()

    def test_wide_directory_does_not_retain_all_entry_names(self):
        class Entry:
            def __init__(self, number):
                self.name = f"capture-{number:020d}"

            def is_symlink(self): return False
            def is_dir(self, *, follow_symlinks=True): return False
            def stat(self, *, follow_symlinks=True): return info

        class Entries:
            def __init__(self): self.entries = (Entry(n) for n in range(100000))
            def __iter__(self): return self
            def __next__(self): return next(self.entries)
            def __enter__(self): return self
            def __exit__(self, *args): self.close()
            def close(self): pass

        info = os.stat_result((stat.S_IFREG | 0o600, 0, 0, 1, 0, 0, 3, 0, 0, 0))
        with mock.patch.object(nas.os, "scandir", side_effect=lambda *args: Entries()), \
                mock.patch.object(Path, "lstat", new=lambda path: info):
            tracemalloc.start()
            try:
                self.assertEqual(self.size(), 300000)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
        self.assertLess(peak, 2 * 1024 ** 2)


if __name__ == "__main__":
    unittest.main()
