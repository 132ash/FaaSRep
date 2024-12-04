import requests

def test_gateway_run():
    url = "http://192.168.162.130:8000/run"  # 根据实际情况修改URL
    payload = {
        "workflow": "testflow",  # 根据实际情况修改请求数据
        "parameters": {"func1":{"chained_num_0":0}}
    }
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        print("Test passed!")
        print("Response:", response.json())
    else:
        print("Test failed!")
        print("Status Code:", response.status_code)
        print("Response:", response.text)

if __name__ == "__main__":
    test_gateway_run()