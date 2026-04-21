import os
import sys
import unittest


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src", "commit_manager"))

from models import UpstreamRef
from serializer_state import BatchCommitTracker, DependencyBuilder, GlobalVersionTable, WriterIndex


class SerializerStateTest(unittest.TestCase):
    def build(self, versions=None):
        version_table = GlobalVersionTable(versions or {})
        writer_index = WriterIndex()
        commit_tracker = BatchCommitTracker()
        builder = DependencyBuilder(version_table, writer_index, commit_tracker)
        return version_table, writer_index, commit_tracker, builder

    def test_stale_read_without_writer_marks_expired(self):
        _, _, _, builder = self.build({"k": "2"})
        result = builder.validate_batch(
            handler_id=0,
            batch_id="b1",
            version="3",
            transaction_list=["t1"],
            read_set_per_batch={"t1": {"f": {"k": "1"}}},
            write_set_per_batch={"t1": {}},
        )

        self.assertEqual(result.expired_keys["t1"]["f"], {"k"})
        self.assertTrue(result.repair_deps["t1"].functions["f"].dirty)
        self.assertEqual(result.repair_deps["t1"].functions["f"].upstream_keys, {})

    def test_cross_batch_writer_dependency(self):
        _, _, _, builder = self.build()
        builder.validate_batch(
            0,
            "b1",
            "1",
            ["t1"],
            {"t1": {}},
            {"t1": {"k": "writer"}},
        )

        result = builder.validate_batch(
            1,
            "b2",
            "2",
            ["t2"],
            {"t2": {"reader": {"k": "0"}}},
            {"t2": {}},
        )

        plan = result.repair_deps["t2"].functions["reader"]
        self.assertEqual(plan.upstream_keys["k"], UpstreamRef("t1", "writer"))
        self.assertEqual(result.pessimistic_sink_info.batch_deps["b1"], {"t2"})
        self.assertEqual(result.pessimistic_sink_info.tx_successors["t1"], {"t2"})

    def test_same_batch_nearest_tx_dependency(self):
        _, _, _, builder = self.build()
        result = builder.validate_batch(
            0,
            "b1",
            "1",
            ["t1", "t2"],
            {"t1": {}, "t2": {"reader": {"k": "0"}}},
            {"t1": {"k": "writer"}, "t2": {}},
        )

        plan = result.repair_deps["t2"].functions["reader"]
        self.assertEqual(plan.upstream_keys["k"], UpstreamRef("t1", "writer"))
        self.assertNotIn("b1", result.pessimistic_sink_info.batch_deps)
        self.assertEqual(result.pessimistic_sink_info.tx_deps["t1"], {"t2"})
        self.assertEqual(result.pessimistic_sink_info.last_tx["t2"], "t1")

    def test_commit_cascade_unblocks_suspended_batch(self):
        _, writer_index, commit_tracker, builder = self.build()
        builder.validate_batch(0, "b1", "1", ["t1"], {"t1": {}}, {"t1": {"k": "f1"}})
        builder.validate_batch(1, "b2", "2", ["t2"], {"t2": {}}, {"t2": {"k": "f2"}})
        commit_tracker.suspend("b2", 1)

        committed = writer_index.pop_committed_writer("k")
        self.assertEqual(committed.tx_id, "t1")
        next_writer = writer_index.first_writer("k")
        self.assertEqual(next_writer.batch_id, "b2")

        became_ready = commit_tracker.mark_key_unblocked("b2")
        self.assertTrue(became_ready)
        self.assertTrue(commit_tracker.pop_suspended_if_ready("b2"))


if __name__ == "__main__":
    unittest.main()
