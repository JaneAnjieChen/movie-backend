from flask import Flask, jsonify
import pymysql
import json
from style_top_2 import check_img_urls
from find_all_style import styles

def get_style_json(style):
    db = pymysql.connect("10.64.132.169", "exp", "DB123", "mscore")

    if style not in styles():
        print('This genre is not documented in the database.')
        exit(0)

    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    sql = " select @maxn:=max(num),@minn:=min(num),@maxr:=max(average_rating),@minr:=min(average_rating) from(select uratings.movieid,title, avg(rating) as average_rating, count(*) as num from (select * from gmovies where genres like '%" \
          + style + "%') as g join uratings on g.movieid=uratings.movieid group by movieid) t;"


    # 根据style拼接查询语句
    sql1 = "select title,genres,average_rating,num,((average_rating-@minr)/(@maxr-@minr)+" \
           "(num-@minn)/(@maxn-@minn))/2 as popularity,img_url" \
           "from (select uratings.movieid,title,genres,img_url,\
           avg(rating) as average_rating,count(*) as num from (select * from gmovies\
           where genres like'%" + style + "%') as g join uratings on g.movieid=uratings.movieid group by g.movieid) t\
           order by popularity desc\
           limit 2;"

    cursor.execute(sql)
    cursor.execute(sql1)
    data = cursor.fetchall()
    # print(data)
    data_dicts = []

    for info in data:
        info_dict = {}
        info_dict['movie_name'] = info[0]
        info_dict['average_rating'] = float(info[2])
        info_dict['rating_num'] = info[3]
        info_dict['popularity_rating'] = float(info[4])
        # info_dict['img_url'] = info[5]

        # 这里找到图片的URL, 查询语句要相应地加上img_url, 没加上的话注释下面这句不然出错
        info_dict['img_url'] = check_img_urls(info[0], info[5])
        data_dicts.append(info_dict)

    return {'movie_list': data_dicts}

# app = Flask(__name__)
# @app.route('/api/style_top_20/<style>')
# def index(style):
#     data_json = get_style_json(style)
#
#     return jsonify(data_json)
#
#
if __name__ == '__main__':
    get_style_json('Comedy')


