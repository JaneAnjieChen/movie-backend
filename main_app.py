from flask import Flask, jsonify
from user_top_20 import get_gender_json
from movie_top_20 import get_style_json
from style_top_2 import get_styles_top_2

app = Flask(__name__)
@app.route('/api/gender_top_20/<gender>')
def index_gender(gender):
    data_json = get_gender_json(gender)
    return jsonify(data_json)


@app.route('/api/style_top_20/<style>')
def index_style(style):
    data_json = get_style_json(style)
    return jsonify(data_json)

# 主页上滚动条怎么得道呢，应该不是跳转吧
@app.route('/api/styles_top_2/')
def index_styles_top_2():
    data_json = get_styles_top_2()
    return jsonify(data_json)

if __name__ == '__main__':
    app.run('0.0.0.0', 5000)
