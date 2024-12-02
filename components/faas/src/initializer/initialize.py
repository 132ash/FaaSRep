import parse_yaml
import sys
import yaml
import component
from workflow_info_repo import Repository

import parse_yaml 

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

def saveWorkflowConfig(workflow, node_info, function_info):
    repo = Repository(workflow.workflow_name)
    repo.save_function_info(function_info, workflow.workflow_name + '_function_info')
    repo.save_start_functions(workflow.start_functions, workflow.workflow_name + '_workflow_metadata')
    repo.save_all_addrs(list(node_info.keys()), workflow.workflow_name + '_workflow_metadata')
   
if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print('usage: python3 initialize.py <workflow_name>, ...')

    node_assign = yaml.load(open("node_assign.yaml"), Loader=yaml.FullLoader)
    node_info = yaml.load(open("worker_info.yaml"), Loader=yaml.FullLoader)

        
    workflow_pool = sys.argv[1:]
    for workflow_name in workflow_pool:
        workflow = parse_yaml.parse(workflow_name)
        function_info = makeWorkflowConfig(workflow, node_assign)
        for name, value in function_info.items():
            print(f"function_name: {name}, value: {value}")
        print(workflow, node_info)
        saveWorkflowConfig(workflow, node_info, function_info)