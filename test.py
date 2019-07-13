import datetime
import time

if __name__ =='__main__':
    u = 17905*24*3600  # unix时间戳
    t = datetime.datetime.fromtimestamp(u)

    ans_time = time.mktime(t.timetuple())
    print(ans_time/86400)