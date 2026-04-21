import os
import sys
import unittest


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src", "commit_manager"))

from validator_state import BatchRuntimeState, ValidatorBatchStore


class ValidatorStateTest(unittest.TestCase):
    def test_batch_runtime_state_collects_parallel_tables(self):
        batch = {
            "transaction_list": ["t1", "t2"],
            "read_set": {"t1": {}, "t2": {"f": {"k": "1"}}},
            "write_set": {"t1": {"k": "f"}, "t2": {}},
            "RYW_subjection": {"t1": {}, "t2": {}},
            "container_port": {"t1": {"f": 20000}, "t2": {"f": 20001}},
        }

        state = BatchRuntimeState.from_validate_payload("b1", batch, 10.0)
        state.mark_repairing()
        state.record_aborts(["t1", "t1"])
        state.mark_repair_finished()

        self.assertEqual(state.transaction_list, ["t1", "t2"])
        self.assertEqual(state.successed_tx_table, {"t2": True})
        self.assertEqual(state.aborted_txs, ["t1"])
        self.assertEqual(state.timestamps[0], 10.0)
        self.assertGreater(state.timestamps[1], 0)
        self.assertGreater(state.timestamps[2], 0)

    def test_batch_store_register_snapshot_and_pop(self):
        state = BatchRuntimeState(
            batch_id="b1",
            transaction_list=["t1"],
            read_set={"t1": {}},
            write_set={"t1": {}},
            ryw_subjection={"t1": {}},
            container_port={"t1": {}},
            first_run_finish_time=1.0,
        )
        store = ValidatorBatchStore()
        store.register(state)

        self.assertIs(store.get("b1"), state)
        snapshots = store.stuck_snapshots(stuck_after=0)
        self.assertEqual(snapshots[0]["batch_id"], "b1")
        self.assertEqual(store.pop("b1"), state)
        self.assertIsNone(store.get("b1"))


if __name__ == "__main__":
    unittest.main()
