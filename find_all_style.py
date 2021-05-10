import pymysql

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
