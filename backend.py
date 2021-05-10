from flask import Flask, request, render_template, jsonify
from flask_cors import *
import time
import pymysql
import json
import requests
import copy
from lxml import etree
import re

app = Flask(__name__)
CORS(app, resources=r'/*')

mysql_path = "10.64.132.169"
mysql_user = "exp"
mysql_pass = "DB123"
mysql_db = "db_exp"


def getImgUrlByMovieName(movieName):
    page = requests.get('https://www.xbsee.com/query/' + movieName)
    selector = etree.HTML(page.text)

    for _ in selector.xpath("//div[@class='col-xs-12 col-sm-6 col-md-6 col-lg-4']"):
        for imgEle in _.xpath(".//img"):
            return 'https:' + imgEle.attrib['src']


# if __name__ == '__main__':
#     res = getImgUrlByMovieName('bbbb')
#     print(res)

# caj
# 传入：tuple[movie_name_index], tuple[img_url_index]
def check_img_urls(movie_name, ori_url):
    '''
    返回电影对应的图片url
    如果有url，直接返回；如果没有，现爬
    '''
    if ori_url is None:
        url = getImgUrlByMovieName(movie_name)
    else:
        url = ori_url
    return url


# 找到所有已经存在的风格，防止非法风格查询
def styles():
    db = pymysql.connect("10.64.132.169", "exp", "DB123", "mscore")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    sql = '''
    select genres from gmovies
    '''
    cursor.execute(sql)
    styles = cursor.fetchall()

    # print(data)
    style_list = []
    for style in styles:
        tmp_list = style[0].split('|')
        for i in tmp_list:
            i = i.strip()
            if i not in style_list:
                style_list.append(i)

    # print(style_list)
    return style_list


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
(select uratings.movieid,avg(rating) as average_rating,
count(*) as num
from (select userid from gusers where gender='Man') man
join uratings on man.userid=uratings.userid
group by movieid) as t join gmovies
on t.movieid=gmovies.movieid;
        """

        male_sql2 = """
   select gmovies.movieid,title,average_rating,num,
((average_rating-@minr)
/(@maxr-@minr)+
(num-@minn)/(@maxn-@minn))/2 
as popularity,img_url
from (select uratings.movieid,
avg(rating) as average_rating,
count(*) as num from
(select userid from gusers where gender='Man') man
join uratings on man.userid=uratings.userid
group by movieid) as t join gmovies
on t.movieid=gmovies.movieid
order by popularity desc
limit 20;
    """

        cursor.execute(male_sql1)
        cursor.execute(male_sql2)
    else:
         # 定义要执行的SQL语句
        female_sql1 = """
      select @maxn:=max(num),@minn:=min(num),
@maxr:=max(average_rating),@minr:=min(average_rating) from
(select uratings.movieid,avg(rating) as average_rating,
count(*) as num
from (select userid from gusers where gender='Female') man
join uratings on man.userid=uratings.userid
group by movieid) as t join gmovies
on t.movieid=gmovies.movieid;
        """

        female_sql2 = """
  select gmovies.movieid,title,average_rating,num,
((average_rating-@minr)
/(@maxr-@minr)+
(num-@minn)/(@maxn-@minn))/2 
as popularity,img_url
from (select uratings.movieid,
avg(rating) as average_rating,
count(*) as num from
(select userid from gusers where gender='Female') man
join uratings on man.userid=uratings.userid
group by movieid) as t join gmovies
on t.movieid=gmovies.movieid
order by popularity desc
limit 20;
    """

        cursor.execute(female_sql1)
        cursor.execute(female_sql2)

    data = cursor.fetchall()

    data_dicts = []

    for info in data:
        info_dict = {}
        info_dict['movie_id'] = info[0]
        info_dict['movie_name'] = info[1]
        info_dict['average_rating'] = float(info[2])
        info_dict['rating_num'] = info[3]
        info_dict['popularity_rating'] = float(info[4])
        # 这里找到图片的URL, 查询语句要相应地加上img_url, 没加上的话注释下面这句不然出错
        info_dict['img_url'] = check_img_urls(info[1], info[5])
        data_dicts.append(info_dict)

    # print(data_dicts)
    return {'movie_list': data_dicts}


def get_style_json(style):
    db = pymysql.connect("10.64.132.169", "exp", "DB123", "mscore")

    if style not in styles():
        print('This genre is not documented in the database.')
        exit(0)

    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    sql = "select @maxn:=max(num),@minn:=min(num),\
            @maxr:=max(average_rating),@minr:=min(average_rating)\
            from(select uratings.movieid,title,\
            avg(rating) as average_rating,\
            count(*) as num from (select * from gmovies\
            where genres like '%"+ style + "%') as g join uratings on g.movieid=uratings.movieid group by movieid) t;"

    # 根据style拼接查询语句
    sql1 = "select title,genres,average_rating,num,\
            ((average_rating-@minr)/(@maxr-@minr)+\
            (num-@minn)/(@maxn-@minn))/2 as popularity,img_url\
            from (select uratings.movieid,title,genres,img_url,avg(rating) as average_rating,\
            count(*) as num from (select * from gmovies where genres like '%" \
           + style + "%') as g join uratings on g.movieid=uratings.movieid group by g.movieid) t \
            order by popularity desc limit 20;"

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


def get_styles_top_2():
    db = pymysql.connect("10.64.132.169", "exp", "DB123", "mscore")

    styles_list = styles()
    data_list = []

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    for style in styles_list:
        sql = "select @maxn:=max(num),@minn:=min(num),\
            @maxr:=max(average_rating),@minr:=min(average_rating)\
            from(select uratings.movieid,title,\
            avg(rating) as average_rating,\
            count(*) as num from (select * from gmovies\
            where genres like '%"+ style + "%') as g join uratings on g.movieid=uratings.movieid group by movieid) t;"
        cursor.execute(sql)
        # 根据style拼接查询语句
        sql1 = "select title,genres,average_rating,num,\
            ((average_rating-@minr)/(@maxr-@minr)+\
            (num-@minn)/(@maxn-@minn))/2 as popularity,img_url\
            from (select uratings.movieid,title,genres,img_url,avg(rating) as average_rating,\
            count(*) as num from (select * from gmovies where genres like '%" \
           + style + "%') as g join uratings on g.movieid=uratings.movieid group by g.movieid) t \
            order by popularity desc limit 2;"
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


# if __name__ == '__main__':
#     print(get_gender_json('male'))
#     print(get_gender_json('female'))
#     print(get_style_json('Drama'))


@app.route('/api/gender_top_20/<gender>')
def index(gender):
    data_json = get_gender_json(gender)
    return jsonify(data_json)


@app.route('/api/style_top_20/<style>')
def index_style(style):
    data_json = get_style_json(style)
    return jsonify(data_json)


# 主页上好看的滚动条，所有风格TOP2
@app.route('/api/styles_top_2/')
def index_styles_top_2():
    data_json = get_styles_top_2()
    return jsonify(data_json)


# zy
def format_timestamp(_timestamp):
    '''
    输入字符串型时间戳
    返回格式化日期：xxxx-xx-xx xx:xx:xx
    '''
    time_array = time.localtime(int(_timestamp))
    return time.strftime("%Y-%m-%d %H:%M:%S", time_array)


# 根目录，保留做调试用
@app.route('/')
def debug():
    return "FLASK!"


@app.route('/api/get_user_rating_info', methods=['POST'])
def get_user_rating_info():
    '''
    实现搜索任务A，根据用户id返回打分信息
    '''
    user_id=request.form['user_id']
    # 连接数据库进行查询
    db = pymysql.connect(mysql_path, mysql_user, mysql_pass, mysql_db)
    cursor = db.cursor()
    sql="""
        select d.title, d.t, d.rating, group_concat(e.tag, ':', c.relevance Separator ';') tag from (
        select a.movieid, a.title, b.rating, b.t 
        from (
        select movieid, rating, from_unixtime(timestamp) as t 
        from uratings where userid={}
        ) as b join gmovies as a on a.movieid=b.movieid
        ) as d left join grelevance as c on c.movieid=d.movieid 
        left join gtags as e on c.tagid=e.tagid
        where d.movieid > 0
        group by d.movieid, d.title, d.t, d.rating 
        order by d.t desc
        limit 20;
        """.format(user_id)

    cursor.execute(sql)
    raw_movie_tuple=cursor.fetchall()
    # 将数据库返回数据处理为json格式
    movie_list=[]; temp_movie_dict={}
    for movie in raw_movie_tuple:
        temp_movie_dict['movie_name']=movie[0]
        temp_movie_dict['rating']=float(movie[2])
        temp_movie_dict['time']=str(movie[1])
        temp_movie_dict['img']='#'
        temp_movie_dict['movie_tag']=[]
        # 处理影片的tag
        if movie[3]:
            tag_dict={}
            for tag in re.split(";", movie[3]):
                temp_tag=re.split("\r:", tag)
                tag_dict[temp_tag[0]]=float(temp_tag[1])
            tags=sorted(tag_dict.items(), reverse = True, key=lambda d: d[1])

            if(len(tags)>3): tags=tags[:3]

            for tag in tags:
                temp_movie_dict['movie_tag'].append(
                    {
                        "tag_name": tag[0],
                        "relevance": tag[1]
                    }
                )

        # 深拷贝
        movie_list.append(copy.deepcopy(temp_movie_dict))
        temp_movie_dict.clear()

    return json.dumps({'movie_list':movie_list},ensure_ascii=False)


@app.route('/api/get_movie_by_keyword', methods=['POST'])
def get_movie_by_keyword():
    '''
    实现搜索任务B，根据关键词返回含关键词的电影
    '''
    keyword=request.form['keyword']
    # 连接数据库进行查询
    db = pymysql.connect(mysql_path, mysql_user, mysql_pass, mysql_db)
    cursor = db.cursor()
    sql="select gmovies.title from gmovies where gmovies.title like '%{}%'".format(keyword)
    cursor.execute(sql)
    movie_tuple=cursor.fetchall()
    # 将数据转为字典
    movie_list=[]

    for i in movie_tuple:
        movie_list.append({
            'movie_name': i[0],
            'img': '#'
        })
    # ensure_ascii=False 保证字符正确显示
    return json.dumps({'movie_list': movie_list},ensure_ascii=False)


if __name__ == '__main__':
    app.run('0.0.0.0', 5000)
