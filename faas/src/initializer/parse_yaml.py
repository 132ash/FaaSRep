import yaml
import component
import sys

sys.path.append('../../config')
import config

yaml_file_addr = config.WORKFLOW_YAML_ADDR

def getYamlFileAddr(option, workflow_name=""):
    if option == "worker_info":
        return f'../../config/worker_info.yaml'
    return f'../../config/workflow_info/{workflow_name}/{option}.yaml'


def parse(workflow_name):
    workflow_data = yaml.load(open(getYamlFileAddr("workflow", workflow_name)), Loader=yaml.FullLoader)
    workflow_functions = workflow_data['functions']
    

    function_nodes = dict()
    start_functions = list()
    end_function = {}
   
    parent_cnt = dict()
    parent_cnt[workflow_functions[0]['name']] = 0
    total = 0


    for function in workflow_functions:
        name = function['name']
        source = function['source']
        next = list()
        input = {}
        output = {}


        if 'input' in function:
            for key in function['input']:
                input[key] = {
                    'from': function['input'][key]['from'],
                    'type': function['input'][key]['type']
                }
        if 'output' in function:
            for key in function['output']:
                output[key] = {
                    'type': function['output'][key]['type'],
                }
        if 'next' in function:
            if function['next']['type'] == 'FINISH':
                if len(end_function) != 0:
                    print("multiple endpoint")
                    exit()
                next.append('END')
                end_function["name"] = name
                end_function["output"] = output
            else:
                for n in function['next']['nodes']:
                    next.append(n)
                    if n not in parent_cnt:
                        parent_cnt[n] = 1
                    else:
                        parent_cnt[n] = parent_cnt[n] + 1
        current_function = component.function(name, next, source, input, output)
        total = total + 1
        function_nodes[name] = current_function

    for name in function_nodes:
        if name not in parent_cnt or parent_cnt[name] == 0:
            parent_cnt[name] = 0
            start_functions.append(name)


    return component.workflow(workflow_name, start_functions, end_function, function_nodes, total, parent_cnt)

if __name__ == '__main__':

    workflow_name = "testFlow"
    workflow = parse(workflow_name)
    print(workflow)