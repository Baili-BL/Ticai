# 主入口文件
from flask import Flask
from routes import api


def create_app():
    app = Flask(__name__)
    # 禁用模板缓存
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.jinja_env.auto_reload = True
    app.register_blueprint(api)
    return app


if __name__ == '__main__':
    app = create_app()
    
    # 启动飞书定时推送任务（每天20:00）
    from feishu_pusher import start_scheduler
    start_scheduler()
    
    # 使用80端口，访问时无需加端口号（Windows需管理员权限）
    app.run(debug=True, host='0.0.0.0', port=80)
