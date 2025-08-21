import parse_yaml
import sys
import yaml
import component
from workflow_info_repo import Repository

import parse_yaml 
from assign_function import assign_function_to_node

def makeWorkflowConfig(workflow: component.workflow, node_assign: list):
    all_function_info = {}
    for func_name in workflow.nodes:
        func = workflow.nodes[func_name]
        func_info = { 'function_name': func_name, 
                      'ip': node_assign[func_name],
                      'parent_cnt': workflow.parent_cnt[func_name],
                      'input': func.input,
                      'output': func.output,
                      'next': func.next
                    }
        all_function_info[func_name] = func_info
    return all_function_info

def saveWorkflowConfig(workflow, all_worker_node, function_info):
    repo = Repository(workflow.workflow_name)
    repo.save_function_info(function_info, workflow.workflow_name + '_function_info')
    repo.save_start_functions(workflow.start_functions, workflow.workflow_name + '_workflow_metadata')
    repo.save_end_function(workflow.end_function, workflow.workflow_name + '_workflow_metadata')
    repo.save_all_addrs(list(all_worker_node), workflow.workflow_name + '_workflow_metadata')
   
if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print('usage: python3 initialize.py <workflow_name>, ...')

    all_worker_node = yaml.load(open(parse_yaml.getYamlFileAddr("worker_info")), Loader=yaml.FullLoader)["nodes"]
    workflow_pool = sys.argv[1:]
    sink_addr_idx = 0
    for workflow_name in workflow_pool:
        sink_node = all_worker_node[sink_addr_idx]
        workflow = parse_yaml.parse(workflow_name)
        node_assign = assign_function_to_node(workflow, all_worker_node, sink_node)
        #print(f"sink_node:{sink_node}, node_assign: {node_assign}")
        function_info = makeWorkflowConfig(workflow, node_assign)
        saveWorkflowConfig(workflow, all_worker_node, function_info)
        sink_addr_idx = (sink_addr_idx + 1) % len(all_worker_node)