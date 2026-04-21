import os
import sys
import unittest


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "config"))
sys.path.insert(0, os.path.join(ROOT, "src", "commit_manager"))
sys.path.insert(0, os.path.join(ROOT, "src", "transaction_sink"))

from repair_info import OPT_REPAIR, RepairInfo
from serializer_state import BatchCommitTracker, DependencyBuilder, GlobalVersionTable, WriterIndex
import validate_struct
from validate_struct import ABORTED, PESSI_REPAIR, REPAIRED, RepairingBatchState


class DeterministicRepairSmokeTest(unittest.TestCase):
    def setUp(self):
        validate_struct.ABORT_PROB = 0

    def build_validation(self):
        versions = GlobalVersionTable()
        writers = WriterIndex()
        commits = BatchCommitTracker()
        builder = DependencyBuilder(versions, writers, commits)
        result = builder.validate_batch(
            0,
            "b1",
            "1",
            ["t1", "t2"],
            {
                "t1": {},
                "t2": {"reader": {"k": "0"}},
            },
            {
                "t1": {"k": "writer"},
                "t2": {},
            },
        )
        return result

    def test_optimistic_repair_flow_without_external_services(self):
        result = self.build_validation()
        expired, crosstx, pessi = result.to_legacy()
        repair_info = RepairInfo(
            None,
            {"writer": ["reader"], "reader": ["END"]},
            {"writer": "worker", "reader": "worker"},
        )
        repair_info.batch_init("b1")
        repair_info.construct_repair_metadata(
            "b1",
            expired,
            crosstx,
            {},
            ["worker"],
            ["t1", "t2"],
            {"t1": {"reader": 1}, "t2": {"reader": 2}},
        )
        metadata = repair_info.get_repair_metadata(OPT_REPAIR, "b1", "worker")
        self.assertEqual(metadata["t2"]["reader"]["upstream_keys"], {"k": ["t1", "writer"]})

        sink = RepairingBatchState("wf")
        sink.register_batch("b1", ["t1", "t2"], 2)
        ready, opt_pessi = sink.update_subjection_info(
            "b1",
            pessi["batch_sub"],
            pessi["tx_sub"],
            pessi["whole_tx_sub"],
        )
        self.assertEqual(ready, {"t1": True})
        self.assertEqual(opt_pessi, {})

        fin, cascaded = sink.after_transaction_finish("b1", OPT_REPAIR, "t1", REPAIRED, False)
        self.assertEqual(fin, {})
        self.assertEqual(cascaded, {})
        fin, cascaded = sink.after_transaction_finish("b1", OPT_REPAIR, "t2", REPAIRED, False)
        self.assertTrue(fin["b1"]["batch_finished"])
        self.assertEqual(cascaded, {})

    def test_pessimistic_fallback_flow_without_external_services(self):
        result = self.build_validation()
        _, _, pessi = result.to_legacy()
        sink = RepairingBatchState("wf")
        sink.register_batch("b1", ["t1", "t2"], 2)
        sink.update_subjection_info(
            "b1",
            pessi["batch_sub"],
            pessi["tx_sub"],
            pessi["whole_tx_sub"],
        )

        sink.after_transaction_finish("b1", OPT_REPAIR, "t2", REPAIRED, False)
        fin, cascaded = sink.after_transaction_finish("b1", OPT_REPAIR, "t1", ABORTED, False)
        self.assertEqual(fin, {})
        self.assertEqual(cascaded["b1"]["pessi_repair_txs"], ["t2"])

        fin, cascaded = sink.after_transaction_finish("b1", PESSI_REPAIR, "t2", REPAIRED, False)
        self.assertTrue(fin["b1"]["batch_finished"])
        self.assertEqual(cascaded, {})


if __name__ == "__main__":
    unittest.main()
