import requests
import os

url = "https://integrate.api.nvidia.com/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {os.getenv('NVIDIA_API_KEY')}",
    "Content-Type": "application/json"
}

# z-ai/glm-5 - 智谱 AI 的 GLM-5 模型
# minimaxai/minimax-m2.1 - MiniMax 的 M2.1 模型
# moonshotai/kimi-k2-thinking - 月之暗面的 Kimi-k2.5 思维模型

data = {
    "model": "z-ai/glm5",
    "messages": [
        {"role": "user", "content": "你好,请介绍一下你自己"}
    ],
    "temperature": 0.7,
    "max_tokens": 1024
}

response = requests.post(url, headers=headers, json=data)
print(response.json())

# 解析响应内容，提取有用的数据信息
response_data = response.json()
if response_data["choices"]:
    message = response_data["choices"][0]["message"]["content"]
    print(message)
