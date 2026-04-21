import os
import sys
import unittest


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "config"))
sys.path.insert(0, os.path.join(ROOT, "src", "commit_manager"))

import config
from repair_info import OPT_REPAIR, PESSI_REPAIR, RepairInfo


class RepairInfoTest(unittest.TestCase):
    def build_repair_info(self):
        workflow_graph = {"a": ["b"], "b": ["END"]}
        function_pos = {"a": "10.0.0.1", "b": "10.0.0.1"}
        info = RepairInfo(None, workflow_graph, function_pos)
        info.fast_path_enabled = True
        return info

    def test_ryw_does_not_clear_existing_dirty_and_removes_same_key_upstream(self):
        repair_info = self.build_repair_info()
        repair_info.batch_init("b1")
        expired_keys = {"t1": {"b": {"k": True, "other": True}}}
        crosstx = {
            "t1": {
                "a": {"dirty": False, "upstream_keys": {}},
                "b": {
                    "dirty": True,
                    "upstream_keys": {
                        "k": ["prev_tx", "prev_func"],
                        "other": ["other_tx", "other_func"],
                    },
                },
            }
        }
        ryw = {"t1": {"b": {"k": "a"}}}

        expired_by_ip = repair_info.construct_repair_metadata(
            "b1",
            expired_keys,
            crosstx,
            ryw,
            ["10.0.0.1"],
            ["t1"],
            {"t1": {"b": 25000}},
        )

        metadata = repair_info.get_repair_metadata(OPT_REPAIR, "b1", "10.0.0.1")
        b_plan = metadata["t1"]["b"]
        self.assertTrue(b_plan["dirty"])
        self.assertEqual(b_plan["RYW_keys"], {"k": "a"})
        self.assertNotIn("k", b_plan["upstream_keys"])
        self.assertEqual(b_plan["upstream_keys"], {"other": ["other_tx", "other_func"]})
        self.assertEqual(b_plan["up_cnt"], 1)
        self.assertEqual(expired_by_ip["10.0.0.1"], {"other"})

    def test_upstream_count_is_deduplicated(self):
        repair_info = self.build_repair_info()
        repair_info.batch_init("b1")
        crosstx = {
            "t1": {
                "b": {
                    "dirty": True,
                    "upstream_keys": {
                        "x": ["same_tx", "same_func"],
                        "y": ["same_tx", "same_func"],
                    },
                }
            }
        }

        repair_info.construct_repair_metadata(
            "b1",
            {"t1": {"b": {}}},
            crosstx,
            {},
            ["10.0.0.1"],
            ["t1"],
            {"t1": {"b": 25000}},
        )

        metadata = repair_info.get_repair_metadata(OPT_REPAIR, "b1", "10.0.0.1")
        self.assertEqual(metadata["t1"]["b"]["up_cnt"], 1)

    def test_pessimistic_metadata_uses_same_default_shape(self):
        repair_info = self.build_repair_info()
        repair_info.batch_init("b1")
        repair_info.construct_repair_metadata(
            "b1",
            {"t1": {}},
            {},
            {"t1": {"b": {"k": "a"}}},
            ["10.0.0.1"],
            ["t1"],
            {"t1": {"b": 25000}},
        )

        expired = {}
        repair_info.update_pessimistic_repair_metadata(
            "b1",
            "t1",
            {"b": {"k": ["tx0", "f0"], "other": None}},
            expired,
        )

        metadata = repair_info.get_repair_metadata(PESSI_REPAIR, "b1", "10.0.0.1")
        b_plan = metadata["t1"]["b"]
        self.assertTrue(b_plan["dirty"])
        self.assertEqual(b_plan["RYW_keys"], {"k": "a"})
        self.assertEqual(b_plan["upstream_keys"], {})
        self.assertEqual(expired["10.0.0.1"], {"other"})


if __name__ == "__main__":
    unittest.main()
