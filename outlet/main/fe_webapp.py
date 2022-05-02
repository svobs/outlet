from flask import Flask
from fe.web.app import MonitorView

if __name__ == '__main__':
    app = Flask(__name__)
    MonitorView.register(app, route_base='/')

    app.run(debug=True, host='0.0.0.0', port=8080)
