# 事务工作流定义和运行

工作流输入由用户传入，gateway接收后写入Databank的global区域（针对每个函数）

支持的参数类型：int、float、string

函数的输入参数只能来自上游调用它的函数(除了global)

不同的分支只能在END处交汇

目前先支持normal和END
END：

```yaml
functions:
  - name: func1
    source: func1
    input: 
        chained_num_0:
            from: GLOBAL
            type: int
    output:
        chained_num_1:
            type: int
    next: 
        type: pass
        nodes:
            - func2_1
            - func2_2
  - name: func2_1
    source: func2_1
    input: 
        chained_num_1:
            from: func1
            type: int
    output:
        chained_num_2:
            type: int
    next: 
      type: pass
      nodes:
        - func3
  - name: func2_2
    source: func2_2
    input: 
       chained_num_1:
            from: func1
            type: int
    output:
        chained_num_final:
            type: int
    next: 
      type: func3
  - name: func3
    source: func3
    input: 
        chained_num_2:
            from: func2_1
            type: int
    output:
        chained_num_final:
            type: int
    next: 
      type: FINISH
```