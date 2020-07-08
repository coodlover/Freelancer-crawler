from bs4 import BeautifulSoup
import requests
import pymysql
import random
my_headers = [
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
    'Opera/9.25 (Windows NT 5.1; U; en)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 "
]

proxy_list = [
    '183.95.80.102:8080',
    '123.160.31.71:8080',
    '115.231.128.79:8080',
    '166.111.77.32:80',
    '43.240.138.31:8080',
    '218.201.98.196:3128'
]


def getheader():
    return {'User-Agent': random.choice(my_headers)}


def getproxy():
    return {'http': 'http://'+random.choice(proxy_list)}

PROXY_POOL_URL = 'http://localhost:5555/random'


def get_proxy():
 try:
    response = requests.get(PROXY_POOL_URL)
    if response.status_code == 200:
        return response.text
 except ConnectionError:
    return None


def getHTML(url):
    proxy = getproxy()
    header = getheader()
    print(proxy)
    print(header)
    try:
        r = requests.get(url, headers=header, proxies=proxy, timeout=200)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        print("获取Url失败:", url)
        return 0


def getProUrl(html, proUrl):
    if html == 0:
        return 0
    soup = BeautifulSoup(html, "html.parser")
    projectList = soup.find(id='project-list')
    proUrlList = []
    for project in projectList.children:
        if project == '\n':
            continue
        status = project.find(class_='JobSearchCard-primary-heading-days').string
        # if status != "已经结束 left":
        #     continue
        url = project.find(class_='JobSearchCard-primary-heading-link')['href']
        proUrlList.append(url)
    print("******", proUrl, "本页共有", len(proUrlList), "个项目")
    return proUrlList


def getDetail(html, url):
    if html == 0:
        return 0
    soup = BeautifulSoup(html, "html.parser")
    detailDic = dict()
    if not soup.find(class_='PageProjectViewLogout-header-title'):
        return None
    detailDic['proName'] = soup.find(class_='PageProjectViewLogout-header-title').string
    detailDic['url'] = url
    print(detailDic['proName'], detailDic['url'], end=" ")
    proDescription = ""
    for s in soup.find(class_='PageProjectViewLogout-detail').stripped_strings:
        proDescription += s + " "
    detailDic['proDescription'] = proDescription
    proTag = ""
    for s in soup.find(class_='PageProjectViewLogout-detail-tags').stripped_strings:
        if s == "技能：":
            continue
        proTag += s + " "
    detailDic['proTag'] = proTag
    detailDic['devList'] = []
    award = soup.find(class_='PageProjectViewLogout-awardedTo')
    if award:
        devDic = dict()
        devBasic = award.find(class_='FreelancerInfo-username')
        devDic['name'] = devBasic.string
        devDic['url'] = devBasic['href']
        devDic['description'] = award.find(class_='FreelancerInfo-about')['data-descr-full']
        devDic['isAward'] = "1"
        detailDic['devList'].append(devDic)
    for dev in soup.find_all(class_='PageProjectViewLogout-freelancerInfo'):
        devDic = dict()
        devBasic = dev.find(class_='FreelancerInfo-username')
        devDic['name'] = devBasic.string
        devDic['url'] = devBasic['href']
        devDic['description'] = dev.find(class_='FreelancerInfo-about')['data-descr-full']
        devDic['isAward'] = "0"
        detailDic['devList'].append(devDic)
    print(" 共有", len(detailDic['devList']), "个开发者", end=" ")
    return detailDic


def sqlExe(db, cursor, sql, params):
    try:
        # 执行sql语句
        cursor.execute(sql, params)
        # 提交到数据库执行
        db.commit()
        return True
    except:
        # 如果发生错误则回滚
        db.rollback()
        print("写入失败:", sql)
        return False

def colon(s):
    return "\'" + s + "\'"

def mysqlWrite(detailDic, db, cursor):
    cursor.execute("select 1 from `project` where `url` = " + colon(detailDic['url']) + " limit 1")
    if len(cursor.fetchall()) != 0:
        print("已写入")
        return
    cursor.execute("SELECT MAX(`MATCH`.id) FROM `MATCH`")
    initID = cursor.fetchall()[0][0]
    match = ";"
    for devDic in detailDic['devList']:
        sql = "INSERT INTO `MATCH`(projectUrl, developerUrl, isAward, description) VALUES (%s,%s,%s,%s)"
        params = (detailDic['url'], devDic['url'], devDic['isAward'], devDic['description'])
        if sqlExe(db, cursor, sql, params):
            initID += 1
            match += str(initID) + ";"
        cursor.execute("select 1 from `developer` where `url` = "+colon(devDic['url'])+" limit 1")
        if len(cursor.fetchall()) == 0:
            sql = "INSERT INTO `developer`(url, name, project) VALUES (%s,%s,%s)"
            params = (devDic['url'], devDic['name'], ";"+detailDic['url']+";")
            sqlExe(db, cursor, sql, params)
        else:
            sql = "update `developer` set `project`=CONCAT(`project`, %s) where `url`=%s"
            params = (detailDic['url']+";", devDic['url'])
            sqlExe(db, cursor, sql, params)
    sql = "INSERT INTO `PROJECT`(url, name, tag, description, `match`) VALUES (%s,%s,%s,%s,%s)"
    params = (detailDic['url'], detailDic['proName'], detailDic['proTag'], detailDic['proDescription'], match)
    sqlExe(db, cursor, sql, params)
    print("写入完成")


# def writelog(page, num):
#     file = open("crawler.log", 'w', encoding="utf-8")
#     file.write(page)
#     file.write(num)
#     file.close()
#
# def readlog():
#     file = open("crawler.log", 'r')
#     page = file.readline().strip()
#     num = file.readline().strip()
#     return page, num

def control():
    db = pymysql.connect("localhost", "root", "1234", "freelancer")
    cursor = db.cursor()
    for i in range(58, 151):
        proUrl = "https://www.freelancer.cn/jobs/" + str(i+1) + "/?status=all&languages=zh"
        proUrlList = getProUrl(getHTML(proUrl), proUrl)
        n = 1
        if proUrlList == 0:
            return
        total = len(proUrlList)
        for url in proUrlList:
            print("{}/{} ".format(n, total), end=" ")
            n += 1
            detailDic = getDetail(getHTML("https://www.freelancer.cn"+url), url)
            if not detailDic:
                continue
            mysqlWrite(detailDic, db, cursor)
            #time.sleep(3)  # 防止系统检测封IP
    db.close()


if __name__ == "__main__":
    control()
