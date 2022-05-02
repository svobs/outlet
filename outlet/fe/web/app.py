from flask import Flask, render_template
from flask_classful import FlaskView, route


class MonitorView(FlaskView):
    def index(self):
        return 'Hello world!'

    @route('/second')
    def second(self):
        return 'Hello second!'
