from flask import Flask, jsonify
import pymysql
import json
from style_top_2 import check_img_urls
# base_route = '/api/gender_top_20'

# get male/female top 20
def get_gender_json(gender):
    db = pymysql.connect("10.64.132.169", "exp", "DB123", "mscore")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    if gender == 'male':
        # 定义要执行的SQL语句
        male_sql1 = """
       select @maxn:=max(num),@minn:=min(num),
    @maxr:=max(average_rating),@minr:=min(average_rating) from
    (select uratings.movieid,title,avg(rating) as average_rating,num
    from (select userid from gusers where gender='Man') man
    join uratings on man.userid=uratings.userid
    join gmovies on gmovies.movieid=uratings.movieid
    join (select movieid,count(*) as num from 
    (select userid from gusers where gender='Man') man
    join uratings on man.userid=uratings.userid
    group by movieid) as s
    on uratings.movieid=s.movieid
    group by movieid order by average_rating desc,num desc
    ) t;
        """

        male_sql2 = """
    select uratings.movieid,title,avg(rating) as average_rating,num,
    ((avg(rating)-@minr)/(@maxr-@minr)+
    (num-@minn)/(@maxn-@minn))/2 as popularity
    from (select userid from gusers where gender='Man') man
    join uratings on man.userid=uratings.userid
    join gmovies on gmovies.movieid=uratings.movieid
    join (select movieid,count(*) as num from 
    (select userid from gusers where gender='Man') man
    join uratings on man.userid=uratings.userid
    group by movieid) as s
    on uratings.movieid=s.movieid
    group by movieid order by popularity desc
    -- average_rating desc,num desc
    limit 20;
    """

        cursor.execute(male_sql1)
        cursor.execute(male_sql2)
    else:
         # 定义要执行的SQL语句
        female_sql1 = """
       select @maxn:=max(num),@minn:=min(num),
    @maxr:=max(average_rating),@minr:=min(average_rating) from
    (select uratings.movieid,title,avg(rating) as average_rating,num
    from (select userid from gusers where gender='Man') man
    join uratings on man.userid=uratings.userid
    join gmovies on gmovies.movieid=uratings.movieid
    join (select movieid,count(*) as num from 
    (select userid from gusers where gender='Man') man
    join uratings on man.userid=uratings.userid
    group by movieid) as s
    on uratings.movieid=s.movieid
    group by movieid order by average_rating desc,num desc
    ) t;
        """

        female_sql2 = """
    select uratings.movieid,title,avg(rating) as average_rating,num,
    ((avg(rating)-@minr)/(@maxr-@minr)+
    (num-@minn)/(@maxn-@minn))/2 as popularity, img_url
    from (select userid from gusers where gender='Man') man
    join uratings on man.userid=uratings.userid
    join gmovies on gmovies.movieid=uratings.movieid
    join (select movieid,count(*) as num from 
    (select userid from gusers where gender='Man') man
    join uratings on man.userid=uratings.userid
    group by movieid) as s
    on uratings.movieid=s.movieid
    group by movieid order by popularity desc
    -- average_rating desc,num desc
    limit 20;
    """

        cursor.execute(female_sql1)
        cursor.execute(female_sql2)

    data = cursor.fetchall()
    # print(male_data)
    # print('=====================================================')
    # data format: (("", "", ""), ())


    # 将male_data 转换成json串的格式：
      #   ```json
      # {
      #     'movie_list':
      #     [
      #         {
      #             'movie_name': title
    #               'movie_id': movieid
    #               'average_rating': average_rating
    #               'ratings_num': num
    #               'popularity_rating': popularity_rating
      #         },
      #         {
      #              ...
      #         }
      #     ]
      # }
      # ```
    data_dicts = []

    for info in data:
        info_dict = {}
        info_dict['movie_id'] = info[0]
        info_dict['movie_name'] = info[1]
        info_dict['average_rating'] = float(info[2])
        info_dict['rating_num'] = info[3]
        info_dict['popularity_rating'] = float(info[4])
        # 这里找到图片的URL, 查询语句要相应地加上img_url, 没加上的话注释下面这句不然出错
        info_dict['img_url'] = check_img_urls(info[0], info[5])
        data_dicts.append(info_dict)

    print(data_dicts)
    return {'movie_list': data_dicts}


# app = Flask(__name__)
# @app.route('/api/gender_top_20/<gender>')
# def index(gender):
#     data_json = get_gender_json(gender)
#
#     return jsonify(data_json)
#
# # @app.route('/')
# # def demo():
# #     return 'Hello'
#
#
# if __name__ == '__main__':
#     # get_gender_json('female')
#     app.run('0.0.0.0', 5000)
