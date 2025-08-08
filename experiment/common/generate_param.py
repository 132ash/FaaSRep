from config import config
import uuid
import random
import json
from datetime import datetime, timedelta
from experiment.common import repository
from pathlib import Path
import numpy as np


script_dir = Path(__file__).parent
repo = repository.Repository()


# travel_reservation parameters
FLIGHT_IDS = config.FLIGHT_IDS
FLIGHT_CAPATICY = config.FLIGHT_CAPACITY
RENTAL_START = config.RENTAL_START
RENTAL_END = config.RENTAL_END
DATE_FORMAT = config.DATE_FORMAT

# mircobenchmark parameters
microbenchmark_dir = script_dir.parent / "microbenchmark"
actual_apps_dir = script_dir.parent / "actual_apps"
TEXT_SIZE_SMALL = 8
TEXT_SIZE_LARGE = 8 * 1024  # 8B / 8KB
DS_JSON_PATH  = microbenchmark_dir / "db_keys.json"

# banking system parameters
BANKING_ACCOUNTS = config.BANKING_ACCOUNTS
BANKING_ORIGINAL_BALANCE = config.BANKING_ORIGINAL_BALANCE
LOGIN_FAIL_PROB = config.LOGIN_FAIL_PROB
BANKING_PWD_DIR = actual_apps_dir / "banking_pwd.json"

# social network parameters
SOCIAL_NETWORK_USERS = config.SOCIAL_NETWORK_USERS
STARTUP_POSTS = config.STARTUP_POSTS
SOCIAL_PWD_DIR = actual_apps_dir / "social_pwd.json"
SOCIAL_POST_IDS_DIR = actual_apps_dir / "social_posts.json"


def generate_banking_system_parameters(client_cnt, round_cnt):
    parameters_inputs = {}
    banking_pwds = json.load(open(BANKING_PWD_DIR, 'r', encoding='utf-8'))
    all_accounts = [f"account_{i}" for i in range(BANKING_ACCOUNTS)]
    amount_options = [100, 200, 500, 1000, 5000, 10000, 20000]
    
    for client_id in range(client_cnt):
        parameters_inputs[client_id] = []
        for round_id in range(round_cnt):
            # 从account_1到account_BANKING_ACCOUNTS中采样src_account和dst_account
            # 确保两个账户不同
            src_account = random.choice(all_accounts)
            # 选择与src_account不同的dst_account
            available_dst_accounts = [acc for acc in all_accounts if acc != src_account]
            dst_account = random.choice(available_dst_accounts)
            
            # 确定密码：有LOGIN_FAIL_PROB的概率使用错误密码
            if random.random() < LOGIN_FAIL_PROB:
                # 使用错误密码（随机生成一个不同的密码）
                correct_password = banking_pwds[src_account]
                # 生成一个与正确密码不同的错误密码
                wrong_password = correct_password + "_wrong"
                password = wrong_password
            else:
                # 使用正确密码
                password = banking_pwds[src_account]
            
            # 随机选择转账金额
            amount = random.choice(amount_options)
            
            parameters_input = {
                'banking_login': {
                    'src_account': src_account,
                    'password': password,
                    'dst_account': dst_account,
                    'amount': amount
                }
            }
            parameters_inputs[client_id].append(parameters_input)
    return parameters_inputs


def generate_travel_reservation_parameters(client_cnt, round_cnt):
    parameters_inputs = {}
    all_flight_ids = [f'flight_{i}' for i in range(FLIGHT_IDS)]

    for client_id in range(client_cnt):
        parameters_inputs[client_id] = []
        for round_id in range(round_cnt):
            transaction_id =  str(uuid.uuid4())
            # Randomly sample a flight_id from all_flight_ids
            selected_flight_id = random.choice(all_flight_ids)

            # Parse date strings and calculate random rental period
            start_date = datetime.strptime(RENTAL_START, DATE_FORMAT)
            end_date = datetime.strptime(RENTAL_END, DATE_FORMAT)

            # Randomly pick a day from [RENTAL_START, RENTAL_END)
            days_range = (end_date - start_date).days
            random_start_offset = random.randint(0, days_range - 1)
            rental_start_date = start_date + timedelta(days=random_start_offset)

            # Randomly choose rental duration from [1, 5] days
            rental_duration = random.randint(1, 5)
            rental_end_date = rental_start_date + timedelta(days=rental_duration)

            # Ensure rental_end doesn't exceed RENTAL_END
            if rental_end_date > end_date:
                rental_end_date = end_date

            # Convert back to string format
            actual_rental_start = rental_start_date.strftime(DATE_FORMAT)
            actual_rental_end = rental_end_date.strftime(DATE_FORMAT)

            parameters_input = {
                'reserve_flight':{
                    'transaction_id': transaction_id,
                    'flight_id': selected_flight_id,
                    'rentle_from': actual_rental_start,
                    'rentle_to': actual_rental_end,
                },
                'transaction_id': transaction_id,
            }
            parameters_inputs[client_id].append(parameters_input)
    return parameters_inputs

def generate_social_media_parameters(client_cnt, round_cnt):
    parameters_inputs = {}
    all_users = [f"user_{i}" for i in range(SOCIAL_NETWORK_USERS)]
    all_posts = json.load(open(SOCIAL_POST_IDS_DIR, 'r', encoding='utf-8'))
    social_pwds = json.load(open(SOCIAL_PWD_DIR, 'r', encoding='utf-8'))

    for client_id in range(client_cnt):
        parameters_inputs[client_id] = []
        for _ in range(round_cnt):
            transaction_id =  str(uuid.uuid4())
            user_id = random.choice(all_users)
            comment_post_id_1, comment_post_id_2, comment_post_id_3 = random.sample(all_posts, 3)
            available_dst_user_id = [acc for acc in all_users if acc != user_id]
            comment_user_id = random.choice(available_dst_user_id)
            # if random.random() < LOGIN_FAIL_PROB:
            #     password = social_pwds[user_id]+ "_wrong"
            # else:
            password = social_pwds[user_id]
            parameters_input = {'social_login':{
                'user_id': user_id,
                'comment_user_id': comment_user_id,
                'comment_post_id_1': comment_post_id_1,
                'comment_post_id_2': comment_post_id_2,
                'comment_post_id_3': comment_post_id_3,
                'password': password,
                'transaction_id': transaction_id,
            }, 'transaction_id': transaction_id,
            }
            parameters_inputs[client_id].append(parameters_input)
    return parameters_inputs

def generate_micro_benchmark_parameters(client_cnt, round_cnt, workflow_parameters):
    dataset_all = json.load(open(DS_JSON_PATH, 'r', encoding='utf-8'))
    workflow = workflow_parameters.get('workflow', {})
    text_size = workflow_parameters.get('text_size', 8)  # Default to 8B if not specified
    dataset = dataset_all['small'] if text_size == TEXT_SIZE_SMALL else dataset_all['large']
    all_func = repo.get_all_functions(workflow)
    client_round_inputs = []
    for client_id in range(client_cnt):
        round_inputs = []
        for round_id in range(round_cnt):
            parameters_input = {'f1': {'payload_size': text_size, 'keys': {func: {} for func in all_func}}}
            for func in all_func:
                zipf_param = 1.1
                dataset_len = len(dataset)
                indices = set()
                while len(indices) < 3:
                    idx = np.random.zipf(zipf_param) - 1
                    if 0 <= idx < dataset_len:
                        indices.add(idx)
                keys = [dataset[i] for i in indices]
                parameters_input['f1']['keys'][func] = {keys[0]: 'R', keys[1]: 'R', keys[2]: 'W'}
            parameters_input['f1']['keys'] = json.dumps(parameters_input['f1']['keys'])
            round_inputs.append(parameters_input)
        client_round_inputs.append(round_inputs)
    return client_round_inputs

def generate_workflow_inputs_for_clients(workflow, client_cnt, round_cnt, workflow_parameters=None):
    if workflow == 'travel_reservation':
        return generate_travel_reservation_parameters(client_cnt, round_cnt)
    elif workflow == 'microbenchmark':
        return generate_micro_benchmark_parameters(client_cnt, round_cnt, workflow_parameters)
    elif workflow == 'banking_system':
        return generate_banking_system_parameters(client_cnt, round_cnt)
    elif workflow == 'social_network':
        return generate_social_media_parameters(client_cnt, round_cnt)
    else:
        raise ValueError(f"Unknown workflow: {workflow}")