from flask import Flask, render_template
from flask_classful import FlaskView, route

from constants import PROJECT_DIR
from util.file_util import get_resource_path


class MonitorView(FlaskView):
    def index(self):
        name = 'Matt'
        return render_template('index.html', name=name)

    @route('/second')
    def second(self):
        return 'Hello second!'
