from flask import Flask
import pymysql


def connect_mysql():
    # Open database connection
    db = pymysql.connect("10.64.132.169", "exp", "DB123", "mscore")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # 定义要执行的SQL语句
    sql = """
    SELECT VERSION()
    """
    # execute SQL query using execute() method.

    cursor.execute(sql)

    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()

    print("Database version : %s " % data)
    # disconnect from server
    db.close()
    return data


app = Flask(__name__)

@app.route('/')
def index():
    data = connect_mysql()
    return '<h1>{}</h1>'.format(data)


# @app.route('/user/<name>')
# def user(name):
#     return '<h1>Hello, {}!</h1>'.format(name)


if __name__ == '__main__':
    # connect_mysql()
    app.run(debug=True)

# app.add_url_rule('/', 'index', index)




