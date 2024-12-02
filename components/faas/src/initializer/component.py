from typing import Dict


class function:
    def __init__(self, name, next, source, input, output):
        self.name = name
        self.next = next
        self.source = source
        self.input = input
        self.output = output

    def __str__(self):
        return f'name: {self.name}, next: {self.next}, source: {self.source}, input: {self.input}, output: {self.output}'


class workflow:
    def __init__(self, workflow_name, start_functions, nodes: Dict[str, function], total, parent_cnt):
        self.workflow_name = workflow_name
        self.start_functions = start_functions
        self.nodes = nodes  # dict: {name: function()}
        # self.global_input = global_input
        self.total = total
        self.parent_cnt = parent_cnt  # dict: {name: parent_cnt}

    def __str__(self) -> str:
        nodes = {}
        for k, v in self.nodes.items():
            nodes[k] = str(v)
        return f'workflow_name: {self.workflow_name}, start_functions: {self.start_functions}, total: {self.total}, parent_cnt: {self.parent_cnt}'