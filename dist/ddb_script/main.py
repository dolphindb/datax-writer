# -*- coding: utf-8 -*-
import json
import os
import sys
import time

# 操作的表
m_table_name = None
# 操作的配置文件
m_op_table_json = None

def get_json_file(file_name):
    '''
        获取json文件内容
    :return:
    '''
    with open(file_name) as json_file:
        config = json.load(json_file)
    return config


def run_command(command,run_type):
    '''
        执行脚本
    :param command: 脚本内容
    :param run_type: 执行类型
    :return:
    '''
    now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if run_type == 'test':
        os.system(command)
    else:

        # 脚本是否异常标记
        is_error = False

        result = os.popen(command)
        res = result.read()
        all_str = ""
        for line in res.splitlines():

            # 有种可能 是警告里面 + 异常    很奇怪
            if line.find('Exception') <> -1 and line.find('WARN') == -1:
                is_error = True

            all_str = all_str + line + "\r\n"

        print all_str

        if is_error:
            try:
                raise BaseException("script run error")
            except Exception as e:
                get_error_info(e)
        else:
            print "script run success!"
            m_op_table_json[m_table_name] = now_time
            write_text_to_file("op_table.json",m_op_table_json)


def create_command(datax,job_json):
    '''
        创建脚本执行脚本
    :param datax: datax所在绝对路径
    :param job_json: job.json 所在绝对路径
    :return:
    '''
    try:
        if not os.path.exists(job_json):
            raise BaseException(job_json + " file not found!")
        if not os.path.exists(datax):
            raise BaseException(datax + " file not found!")
        create_run_job_json(job_json)
    except Exception as e:
        get_error_info(e)

    return "python " + datax + " " + os.getcwd() + "/run_job.json"

def create_run_job_json(source_job_josn_path):
    '''
        创建需要执行的job.json文件
    :return:
    '''
    source_job_josn = get_json_file(source_job_josn_path)
    op_table_json = get_json_file('op_table.json')
    global m_op_table_json
    m_op_table_json = op_table_json

    op_table = source_job_josn['job']['content'][0]['reader']['parameter']['connection'][0]['table'][0]
    global m_table_name
    m_table_name = op_table

    if op_table_json.has_key(op_table):
        print "增量导入"
        source_job_josn['job']['content'][0]['reader']['parameter']['where'] = "OPMODE = 0 AND OPDATE > to_date('"+ op_table_json[op_table] +"', 'YYYY-MM-DD HH24:MI:SS')"
    else:
        print "全量导入"

    write_text_to_file('run_job.json',source_job_josn)

def write_text_to_file(filename,json_str):
    '''
        向文件写入内容
    :param filename: 文件名
    :param json_str: 内容
    :return:
    '''
    with open(filename, 'w') as file_object:
        file_object.write(json.dumps(json_str))

class BaseException(Exception):

    def __init__(self, message):
        self.message = message

def get_error_info(e):
    '''

        获取异常具体信息

    :param e: Exception
    :return: jsonstr
    '''
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback_details = {
        'filename': exc_traceback.tb_frame.f_code.co_filename,
        'lineno': exc_traceback.tb_lineno,
        'type': exc_type.__name__,
        'msg' : ""
    }
    if traceback_details['type'] == 'BaseException':
        traceback_details['msg'] = e.message
    else:
        if len(e.args) > 0:
            traceback_details['msg'] = e.args[0]

    # info_json = json.dumps(traceback_details,ensure_ascii=False)
    print "ERROR:" + traceback_details['msg']


def run(datax_path,job_json,run_type):
    run_command(create_command(datax_path,job_json),run_type)


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "please input param datax_path or job_path！\r\n" + "examples：" + "python main.py datax job.json"
    else:
        datax_path = sys.argv[1]
        job_path = sys.argv[2]
        if len(sys.argv) == 4:
           run_type = sys.argv[3]
        else:
           run_type = 'test'

        run(datax_path, job_path,run_type)





