# 主入口文件
from flask import Flask
from routes import api


def create_app():
    app = Flask(__name__)
    app.register_blueprint(api)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, port=5000)
