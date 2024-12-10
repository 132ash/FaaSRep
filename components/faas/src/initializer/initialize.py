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
    repo.save_end_function(workflow.end_function, workflow.workflow_name + '_workflow_metadata')
    repo.save_all_addrs(list(node_info), workflow.workflow_name + '_workflow_metadata')
     
def saveNodeInfoGlobal(node_info):
    repo = Repository()
    repo.save_all_addrs(node_info, 'common')
   
if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print('usage: python3 initialize.py <workflow_name>, ...')

    node_info = yaml.load(open(parse_yaml.getYamlFileAddr("worker_info")), Loader=yaml.FullLoader)["nodes"]

    saveNodeInfoGlobal(node_info)
    workflow_pool = sys.argv[1:]
    for workflow_name in workflow_pool:
        node_assign = yaml.load(open(parse_yaml.getYamlFileAddr("node_assign", workflow_name)), Loader=yaml.FullLoader)
        workflow = parse_yaml.parse(workflow_name)
        function_info = makeWorkflowConfig(workflow, node_assign)
        saveWorkflowConfig(workflow, node_info, function_info)