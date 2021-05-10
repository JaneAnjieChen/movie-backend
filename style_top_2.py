from flask import Flask, jsonify
import pymysql
import json
from img_url_crawler import getImgUrlByMovieName
from find_all_style import styles

# 传入：tuple[movie_name_index], tuple[img_url_index]
def check_img_urls(movie_name, ori_url):
    if ori_url is None:
        url = getImgUrlByMovieName(movie_name)
    else:
        url = ori_url
    return url

def get_styles_top_2():
    db = pymysql.connect("10.64.132.169", "exp", "DB123", "mscore")

    styles_list = styles()
    data_list = []

    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    sql = '''
    select @maxn:=max(num),@minn:=min(num),
@maxr:=max(average_rating),@minr:=min(average_rating) from
(select uratings.movieid,title,avg(rating) as average_rating,num
from uratings join gmovies on gmovies.movieid=uratings.movieid
join (select movieid,count(*) as num from uratings group by movieid) as s
on uratings.movieid=s.movieid
group by movieid order by average_rating desc,num desc
) t;
    '''
    cursor.execute(sql)
    for style in styles_list:
        # 根据style拼接查询语句
        sql1 = "select title,genres,avg(rating) as average_rating,num,\
        ((avg(rating)-@minr)/(@maxr-@minr)+\
        (num-@minn)/(@maxn-@minn))/2 as popularity, img_url\
        from uratings join gmovies on gmovies.movieid=uratings.movieid\
        join (select movieid,count(*) as num from uratings group by movieid) as s\
        on uratings.movieid=s.movieid\
        where genres like '%" + style + "%'group by gmovies.movieid order by popularity desc\
        limit 2;"
        cursor.execute(sql1)
        data = cursor.fetchall()
        # print(data)
        for info in data:
            data_list.append(info)

    data_dicts = []
    for info in data_list:
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
# @app.route('/api/styles_top_2/')
# def index():
#     data_json = get_styles_top_2()
#
#     return jsonify(data_json)
#
#
# if __name__ == '__main__':
#     # get_styles_top_2()
#     app.run('0.0.0.0', 5000)
