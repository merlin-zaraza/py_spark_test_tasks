"""
Flask App to execute test tasks
"""
import os

from multiprocessing import Process
from subprocess import Popen, PIPE

from flask import Flask, request, render_template, Response, redirect, flash

from waitress import serve
from pyspark_task import LIST_ALL_TASKS

_main_html: str = "main.html"
_SUBPROCESS: {str: Popen} = {}
_server: Process

# Flask constructor
app = Flask(__name__, static_folder="static/", template_folder="templates/")
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

LIST_ALL_TASKS_STR = [f"{task.group_id},{task.task_id}" for task in LIST_ALL_TASKS]


# which URL is associated function
@app.route('/', methods=["POST", "GET"])
def main():
    """
    App entry point
    :return: redirect
    """
    return redirect("/run_task", code=307)


@app.route('/run_task', methods=["POST", "GET"])
def run_task():
    """
    function to run task based on form parameters
    :return: redirect
    """
    if request.method == "GET":
        return render_template(_main_html,
                               all_tasks_list=LIST_ALL_TASKS_STR)

    l_task = request.form.get("in_task")
    l_task_type = request.form.get("in_task_type")

    return redirect(f"/run_task/task={l_task}&task_type={l_task_type}", code=307)


@app.route('/run_task/task=<in_task>&task_type=<in_task_type>', methods=["POST", "GET"])
def run_task_by_id(in_task: str, in_task_type: str):
    """
    function to run task based on url parameters
    :return: render_template
    """
    l_task = in_task.split(",")

    l_group_id = l_task[0]
    l_task_id = l_task[1]

    _SUBPROCESS.setdefault(in_task, None)

    if _SUBPROCESS[in_task]:
        flash(f"Task {in_task} has been already started", 'error')
    else:
        os.environ["PYTHONUNBUFFERED"] = "1"

        _SUBPROCESS[in_task] = Popen(['spark-submit', 'pyspark_task.py',
                                      "-g", l_group_id,
                                      "-t", l_task_id,
                                      "-tt", in_task_type],
                                     cwd=os.environ.get("SPARK_APPS"),
                                     stdout=PIPE,
                                     stderr=PIPE, )

    return render_template(_main_html,
                           in_task=in_task,
                           in_task_type=in_task_type,
                           all_tasks_list=LIST_ALL_TASKS_STR,
                           selected_task=in_task)


def flask_logger(in_task):
    """creates logging information"""

    if in_task not in [k for k, v in _SUBPROCESS.items()]:
        yield f"Task {in_task} execution has not been started"
    else:
        l_subprocess = _SUBPROCESS[in_task]

        for l_output in [l_subprocess.stdout, l_subprocess.stderr]:

            l_line = l_output.readline()

            while l_line:

                if l_line:
                    yield l_line.rstrip() + "\n".encode()

                l_line = l_output.readline()

            l_output.close()

        l_subprocess.wait()

        del _SUBPROCESS[in_task]


@app.route("/log_stream/<in_task>", methods=["GET"])
def log_stream(in_task):
    """returns logging information"""
    return Response(flask_logger(in_task), mimetype="text/plain", content_type="text/event-stream")


def create_app():
    """ Start app using waitress """
    serve(app, host='0.0.0.0', port=5000)


if __name__ == '__main__':
    create_app()
    # app.run(host='0.0.0.0', port=5000, debug=True)
