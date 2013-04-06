
gpars是groovy很漂亮的并发框架，实现了fork-join, actor, dataflow, agent, CSP 等多种并发模式。

我对Gpars的使用：

    dataflow_operator，使用DataflowOperator。用于ETL引擎，静态计算引擎。

    dataflow_and_actor，使用DataflowVariable，Actor。用于情景模拟引擎。

