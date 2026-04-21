import os
import sys
import unittest


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "config"))
sys.path.insert(0, os.path.join(ROOT, "src", "commit_manager"))

from pessimistic_repairer import PessimisticRepairer


class FakeRepairInfo:
    def __init__(self):
        self.calls = []

    def update_pessimistic_repair_metadata(self, batch_id, tx_id, tx_dependency, expired_keys):
        self.calls.append((batch_id, tx_id, tx_dependency))
        for func, deps in tx_dependency.items():
            for key, dep in deps.items():
                if dep is None:
                    expired_keys.setdefault("worker", set()).add(key)


class PessimisticRepairerTest(unittest.TestCase):
    def test_abort_removes_writer_and_is_idempotent(self):
        repair_info = FakeRepairInfo()
        repairer = PessimisticRepairer(None, "wf", repair_info)
        read_set = {
            "tx1": {},
            "tx2": {},
            "tx3": {"reader": {"k1": "0", "k2": "0"}},
        }
        write_set = {
            "tx1": {"k1": "f1"},
            "tx2": {"k1": "f2", "k2": "f2"},
            "tx3": {},
        }
        success = {"tx1": True, "tx2": True, "tx3": True}

        repairer.register_repair_info(
            "b1",
            read_set,
            write_set,
            ["tx1", "tx2", "tx3"],
            {"tx3": "tx2"},
        )
        repairer.modify_batch_write_table_for_abort("b1", ["tx2", "tx2"], write_set, success)

        expired = {}
        repairer.prepare_pessimistic_info("b1", expired, ["tx3"])

        _, _, dependency = repair_info.calls[-1]
        self.assertEqual(dependency["reader"]["k1"], ["tx1", "f1"])
        self.assertIsNone(dependency["reader"]["k2"])
        self.assertEqual(expired["worker"], {"k2"})
        self.assertEqual(repairer.pessimistic_get_commit_keys("b1"), {"k1"})
        self.assertNotIn("tx2", success)

    def test_duplicate_dependencies_are_left_for_metadata_dedup(self):
        repair_info = FakeRepairInfo()
        repairer = PessimisticRepairer(None, "wf", repair_info)
        read_set = {
            "tx1": {},
            "tx2": {"reader": {"k1": "0", "k2": "0"}},
        }
        write_set = {
            "tx1": {"k1": "f1", "k2": "f1"},
            "tx2": {},
        }
        repairer.register_repair_info(
            "b1",
            read_set,
            write_set,
            ["tx1", "tx2"],
            {"tx2": "tx1"},
        )

        repairer.prepare_pessimistic_info("b1", {}, ["tx2"])

        _, _, dependency = repair_info.calls[-1]
        self.assertEqual(dependency["reader"]["k1"], ["tx1", "f1"])
        self.assertEqual(dependency["reader"]["k2"], ["tx1", "f1"])


if __name__ == "__main__":
    unittest.main()
