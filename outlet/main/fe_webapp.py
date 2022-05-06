from flask import Flask

from constants import PROJECT_DIR
from fe.web.app import MonitorView
from util.file_util import get_resource_path

if __name__ == '__main__':
    template_dir = get_resource_path(f'{PROJECT_DIR}/template')
    app = Flask(__name__, template_folder=template_dir)
    MonitorView.register(app, route_base='/')

    app.run(debug=True, host='0.0.0.0', port=8080)
