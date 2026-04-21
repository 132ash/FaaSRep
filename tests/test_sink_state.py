import os
import sys
import unittest


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "config"))
sys.path.insert(0, os.path.join(ROOT, "src", "transaction_sink"))

import validate_struct
from validate_struct import ABORTED, REPAIRED, OPT_REPAIR, PESSI_REPAIR, RepairingBatchState


class SinkStateTest(unittest.TestCase):
    def setUp(self):
        validate_struct.ABORT_PROB = 0

    def test_late_optimistic_finish_after_abort_triggers_or_waits_for_pessimistic(self):
        state = RepairingBatchState("wf")
        state.register_batch("b1", ["t1", "t2"], 2)
        ready, opt_pessi = state.update_subjection_info(
            "b1",
            {},
            {"t1": ["t2"]},
            {"t1": {"t2": True}},
        )
        self.assertEqual(ready, {"t1": True})
        self.assertEqual(opt_pessi, {})

        fin, cascaded = state.after_transaction_finish("b1", OPT_REPAIR, "t2", REPAIRED, False)
        self.assertEqual(fin, {})
        self.assertEqual(cascaded, {})

        fin, cascaded = state.after_transaction_finish("b1", OPT_REPAIR, "t1", ABORTED, False)
        self.assertEqual(fin, {})
        self.assertEqual(cascaded["b1"]["pessi_repair_txs"], ["t2"])
        self.assertEqual(cascaded["b1"]["aborted_txs"], ["t1"])

        fin, cascaded = state.after_transaction_finish("b1", OPT_REPAIR, "t2", REPAIRED, False)
        self.assertEqual(fin, {})
        self.assertEqual(cascaded, {})

        fin, cascaded = state.after_transaction_finish("b1", PESSI_REPAIR, "t2", REPAIRED, False)
        self.assertTrue(fin["b1"]["batch_finished"])
        self.assertEqual(cascaded, {})

    def test_duplicate_or_late_finish_is_idempotent_after_batch_cleanup(self):
        state = RepairingBatchState("wf")
        state.register_batch("b1", ["t1"], 1)
        state.update_subjection_info("b1", {}, {}, {})

        fin, cascaded = state.after_transaction_finish("b1", OPT_REPAIR, "t1", REPAIRED, False)
        self.assertTrue(fin["b1"]["batch_finished"])
        self.assertEqual(cascaded, {})

        fin, cascaded = state.after_transaction_finish("b1", OPT_REPAIR, "t1", REPAIRED, False)
        self.assertEqual(fin, {})
        self.assertEqual(cascaded, {})

    def test_unknown_predecessor_is_recorded_and_forces_pessimistic_path(self):
        state = RepairingBatchState("wf")
        state.register_batch("b2", ["t2"], 1)

        ready, opt_pessi = state.update_subjection_info(
            "b2",
            {"missing_batch": ["t2"]},
            {},
            {},
        )

        self.assertEqual(ready, {"t2": True})
        self.assertEqual(opt_pessi, {"t2": True})
        self.assertIn("unknown_batch:missing_batch", state.unknown_predecessors["b2"])
        self.assertTrue(state.transaction_state_per_tx["t2"].needs_pessimistic)


if __name__ == "__main__":
    unittest.main()
