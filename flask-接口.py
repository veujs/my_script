from flask import Flask, request, abort, Response
import json
app = Flask(__name__)
API_HOST = "127.0.0.1"
API_PORT = 9000


def api():
    print("接口开启")
    app.run(host=API_HOST, port=API_PORT)


@app.route('/')
def index():
    return '<h2>Welcome to judgement System</h2>'


@app.route('/judge/', methods=["POST", "GET"])
def judge():
    if request.method == 'POST':
        rev_data = json.loads(request.data)
        # 数据格式
        """
        {
            'text': '待识别的内容'
        }    
        """
        text = rev_data.pop('text', None)

        if text:
            """
            执行识别的功能函数
            ..
            ..
            ..
            """
            print(text)
            data = {"result": "ok", "judgement": '待填充的识别结果'}
            return Response(json.dumps(data))
        else:
            data = {"result": "failed", "msg": "text内容为空！"}
            return Response(json.dumps(data))

    else:
        text = request.args.get('text')
        if text:
            """
            执行识别的功能函数
            """

            data = {"result": "ok", "judgement": '待填充的识别结果'}
            return Response(data)
        else:
            data = {"result": "failed", "msg": "text内容为空！"}
            return Response(data)


if __name__ == '__main__':
    api()



