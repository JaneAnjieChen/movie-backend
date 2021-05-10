import requests
from lxml import etree

def getImgUrlByMovieName(movieName):
    page = requests.get('https://www.xbsee.com/query/' + movieName)
    selector = etree.HTML(page.text)
    res = []
    for _ in selector.xpath("//div[@class='col-xs-12 col-sm-6 col-md-6 col-lg-4']"):
        for imgEle in _.xpath(".//img"):
            res.append('https:' + imgEle.attrib['src'])
    return res

if __name__ == '__main__':
    # print(getImgUrlByMovieName('姜子牙'))
    # for _ in getImgUrlByMovieName('姜子牙'):
    #     print(_)
    url = getImgUrlByMovieName('hahaha')[0]
    print(url)

