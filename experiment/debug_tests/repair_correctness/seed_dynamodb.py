from __future__ import annotations

import argparse

from correctness_lib import get_data_item, put_data_items


DEFAULT_ITEMS = {
    "rc_hot": "0",
    "rc_tail": "seed:0",
    "rc_guard": "0",
    "rc_audit": "seed:audit",
    "rc_result": "seed:result",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed DynamoDB items for repair_correctness.")
    parser.add_argument("--endpoint-url", default=None)
    parser.add_argument("--hot", default="0")
    parser.add_argument("--tail", default="seed:0")
    parser.add_argument("--guard", default="0")
    parser.add_argument("--audit", default="seed:audit")
    parser.add_argument("--result", default="seed:result")
    args = parser.parse_args()

    items = dict(DEFAULT_ITEMS)
    items["rc_hot"] = args.hot
    items["rc_tail"] = args.tail
    items["rc_guard"] = args.guard
    items["rc_audit"] = args.audit
    items["rc_result"] = args.result
    put_data_items(items, args.endpoint_url)
    for key in sorted(items):
        item = get_data_item(key, args.endpoint_url)
        print(f"{key}={item['value']} version={item['version']}")


if __name__ == "__main__":
    main()
