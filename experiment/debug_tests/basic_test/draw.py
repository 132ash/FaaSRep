import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 读取 CSV 文件
file_path = "all.csv"
data = pd.read_csv(file_path)

# 筛选数据
cache_data = data[data['condition'] == 'Cache']
nocache_data = data[data['condition'] == 'NoCache']

# 定义绘图函数
def plot_bar_chart(data, title, output_file):
    # 按照指定顺序排列模式
    modes = ['lock', 'repair', 'repair+batch', 'repair+batch+fastpath']
    data = data[data['mode'].isin(modes)]
    data = data.set_index('mode').loc[modes]

    # 属性列
    attributes = ['validator overhead', 'overall validate latency', 'e2e latency', 
                  'workflow run latency', 'func io latency', 'func exec latency']

    # 绘制柱状图
    x = np.arange(len(modes))  # x 轴位置
    width = 0.15  # 每个柱子的宽度

    fig, ax = plt.subplots(figsize=(12, 6))
    for i, attr in enumerate(attributes):
        ax.bar(x + i * width, data[attr], width, label=attr)

    # 设置图表信息
    ax.set_xlabel('Mode')
    ax.set_ylabel('Values')
    ax.set_title(title)
    ax.set_xticks(x + width * (len(attributes) - 1) / 2)
    ax.set_xticklabels(modes)
    ax.legend()

    # 保存图表
    plt.tight_layout()
    plt.savefig(output_file)
    plt.show()

# 绘制第一张图：condition=Cache
plot_bar_chart(cache_data, 'Condition: Cache', 'cache_bar_chart.png')

# 绘制第二张图：condition=NoCache
plot_bar_chart(nocache_data, 'Condition: NoCache', 'nocache_bar_chart.png')