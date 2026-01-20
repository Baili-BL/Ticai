# 主入口文件
import os
from flask import Flask
from routes import api
from database import init_database


def create_app():
    app = Flask(__name__)
    # 禁用模板缓存
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.jinja_env.auto_reload = True
    app.register_blueprint(api)
    return app


if __name__ == '__main__':
    # 初始化数据库
    init_database()
    
    app = create_app()
    
    # 只在主进程启动定时任务（避免debug模式下重复启动）
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug:
        from feishu_pusher import start_scheduler
        start_scheduler()
    
    # 启动收益跟踪定时任务（每天15:30）
    from performance_tracker import start_performance_scheduler
    start_performance_scheduler()
    
    # 使用5002端口（80端口被Nginx占用）
    import os
    port = int(os.environ.get('PORT', 5002))
    app.run(debug=True, host='0.0.0.0', port=port)
