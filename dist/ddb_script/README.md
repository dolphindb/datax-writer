1. 文件内容

   op_table.json  同步数据后的日志 记录每个表最后操作时间
        # 格式 --------->  表名 : 最后同步的时间

   run_job.json   临时执行文件，脚本会拷贝需要执行的job.json的内容到此文件进行修改

2. 设计思路

    每次执行脚本时脚本会先读取op_table.json内容

       如果没有本次操作所涉及的表
       脚本会执行全量操作
       操作完成后会向op_table.json 写入 本次所执行的表和时间(表名 : 最后同步的时间)

       如果发现有本次操作的表
       脚本会修改 job 配置文件(只修改run_job.json临时配置文件，不会修改原始job配置文件)
       加入  检索条件 OPMODE = 0 AND OPDATE > to_date('最后同步时间', 'YYYY-MM-DD HH24:MI:SS')
       并执行增量操作 操作完成后会向op_table.json 跟新 本次所执行时间(表名 : 最后同步的时间)

3. 脚本执行

    python main datax.py(Absolute Path) job.json(Absolute Path) run_type(test/prod)

    例 python main.py /Users/apple/Documents/datax/datax/bin/datax.py /Users/apple/Documents/datax/datax/job/AINDEXALTERNATIVEMEMBERS.json test

    run_type:
        设置为test 是 脚本实时打印 datax 输出的内容，
        但是程序无法读取到datax 输出内容，因此程序无法发现datax输出中是否有Exception
        此设置不会向op_table.json 进行操作 仅供调试使用

        设置为prod 是 脚本无法实时打印 datax 输出内容,但程序可以读取到datax输出内容从而可以判断输出中的Exception
        此设置会向op_table.json 添加/更新 操作时间  用于真实环境




