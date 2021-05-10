from flask import Flask, request, render_template, jsonify
from flask_cors import *
import time
import pymysql
import json
import requests
import copy
from lxml import etree

app = Flask(__name__)
CORS(app, resources=r'/*')

mysql_path="10.64.132.169"
mysql_user="exp"
mysql_pass="DB123"
mysql_db="mscore"

# 传入：tuple[movie_name_index], tuple[img_url_index]
def check_img_urls(movie_name, ori_url):
    '''
    返回电影对应的图片url
    '''
    if ori_url is None:
        url = getImgUrlByMovieName(movie_name)
    else:
        url = ori_url
    return url

def getImgUrlByMovieName(movieName):
    '''
    爬取电影的图片url
    '''
    page = requests.get('https://www.xbsee.com/query/' + movieName)
    selector = etree.HTML(page.text)
    res = []
    for _ in selector.xpath("//a[@title='" + movieName + "']"):
        for imgEle in _.xpath(".//img"):
            res.append('https:' + imgEle.attrib['src'])
    return res[0]

def get_gender_json(gender):
    '''
    实现搜索任务D，根据性别返回top20电影列表数据
    '''
    db=pymysql.connect(mysql_path, mysql_user, mysql_pass, mysql_db)

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

        data_dicts.append(info_dict)

    return {'movie_list': data_dicts}

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
    sql_1="set @movieid:=null, @rownum:=0;"
    sql_2="""
        select title,rating,
        tag,relevance,timestamp from gmovies join 
        (select movieid,rating,timestamp from 
        (select movieid,rating,timestamp from uratings 
        where userid={}) id) newtable
        on gmovies.movieid=newtable.movieid
        left join (select movieid,T.tagid,tag,relevance from
        (select t.*,
        @rownum:= (case when @movieid = t.movieid 
        then @rownum + 1 else 1 end) count,
        @movieid:=t.movieid
        from grelevance t 
        order by t.movieid asc, t.relevance desc) T 
        join gtags on T.tagid=gtags.tagid
        where count<4)g 
        on g.movieid=gmovies.movieid
        order by from_unixtime(timestamp) desc;
        """.format(user_id)

    cursor.execute(sql_1)
    cursor.execute(sql_2)
    raw_movie_tuple=cursor.fetchall()
    # 将数据库返回数据处理为json格式
    movie_list=[]; i=0; temp_movie_dict={}
    prior_movie_name=""
    length=len(raw_movie_tuple)

    while(i<length):
        movie_info=raw_movie_tuple[i]
        if('movie_name' not in temp_movie_dict.keys()): 
            temp_movie_dict['movie_name']=movie_info[0]
            temp_movie_dict['rating']=float(movie_info[1])
            temp_movie_dict['time']=format_timestamp(movie_info[4])
            temp_movie_dict['img']='#'
            temp_movie_dict['movie_tag']=[]
            prior_movie_name=movie_info[0]

        while(movie_info[0] == prior_movie_name):
            temp_tag_dict={}
            temp_tag_dict['tag_name']= movie_info[2][:-1] if (movie_info[2]) else None
            temp_tag_dict['tag_relevance']=movie_info[3]
            temp_movie_dict['movie_tag'].append(temp_tag_dict)
            i=i+1
            if(i>=length):break
            movie_info=raw_movie_tuple[i]
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
    return json.dumps({'movie_list':movie_list},ensure_ascii=False)

@app.route('/api/gender_top_20/<gender>')
def index(gender):
    data_json = get_gender_json(gender)

    return jsonify(data_json)

if __name__ == '__main__':
    app.run('0.0.0.0', 5000)
