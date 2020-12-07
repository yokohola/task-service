import os
from task import TaskManager, Task, add_to_scheduler
from flask_expects_json import expects_json
from flask import Flask, request, jsonify, send_from_directory
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler

__all__ = ['app']
__dev__ = 'yokohola'
__ver__ = '0.1a'


app = Flask(__name__)
app.secret_key = os.urandom(24)

FILE_DIR = 'files'

if not os.path.exists(FILE_DIR):
    os.mkdir(FILE_DIR)

# ------------------------------------------------ #
# Init background scheduler for processing tasks
# & set background executors.
# ------------------------------------------------ #
executors = {'default': ThreadPoolExecutor(8)}
scheduler = BackgroundScheduler(executors=executors)
scheduler.start()

# ------------------------------------------------ #
# Init managers
# ------------------------------------------------ #
manager = TaskManager()
add_to_scheduler(scheduler, manager)

# ------------------------------------------------ #
# Init schema for validation input data
# ------------------------------------------------ #
schema = {
    'type': 'object',
    'properties': {
        'url': {'type': 'string'},
    },
    'required': ['url']
}


@app.route('/api/tasks/', methods=('POST',))
@expects_json(schema)
def route_create_task():
    """Create task for parsing site. `Url` required."""
    data = request.get_json()
    task = Task(**data)
    manager.add_task(task)
    return jsonify(task.json()), 200


@app.route('/api/tasks/<task_id>/', methods=('GET',))
def route_get_task(task_id):
    """Get and return task by uuid, returns 404 if task does not exist."""
    task = manager.get_task(task_id)
    if not task:
        return jsonify({'detail': 'Not found'}), 404
    return jsonify(task.json()), 200


@app.route('/files/<path:filename>/', methods=('GET',))
def download_file(filename):
    """Download file by name, if it does not exist - raises 404."""
    return send_from_directory(
        FILE_DIR,
        filename,
        as_attachment=True
    )


if __name__ == '__main__':
    app.run(debug=True)
