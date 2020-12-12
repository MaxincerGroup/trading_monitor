from flask import Flask
app = Flask(__name__)

# 装饰器的作用是将路由映射到视图函数index
@app.route('/')
def index():
    return 'ok'

if __name__ == '__main__':
    app.run()