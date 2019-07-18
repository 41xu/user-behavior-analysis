import pandas as pd
import time
import datetime


def timecal_funnel(t, num):  # t-> "2008-01-01"
    t += " 00:00:00"
    # 70s生成一个时间戳
    from_time = int(time.mktime(time.strptime(t, "%Y-%m-%d %H:%M:%S")))
    times = []
    days = []
    for i in range(num):
        times.append(from_time + i * 70)
    # for x in times:
    #     days.append(x // 86400)
    days = [from_time//86400]*len(times)
    for i in range(len(times)):
        times[i] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(times[i]))
        times[i] += ".000000000"
    last_time = times[-1][:-19]
    return times, days, last_time


# 每名用户隔5条又出现一次
def user_funnel(num, start):
    users = []
    for i in range(start, start + num):
        temp = []
        for j in range(5):
            temp.append(j + i * 5)
        users += temp * 4
    return users


# 因为每名用户隔5条出现一次，一共出现4次，而我们设置的漏斗查询的时间是2h，所以上面的时间戳生成函数为20min生成一个时间戳，这样保证100<120

# 漏斗分析注册流程的event_id生成,其中
# 1->2设成96%（240/250）的用户完成行为, 剩余10名用户的步骤2event_id被设置成26（没有完成下一步，而是点击注册两次）
# 2->3设成87.5%（210/240）的用户完成行为，剩余30名用户event_id被设置成88（app退出）
# 3->4设成85.7%（180/210）的用户完成行为，剩余30名用户event_id被设置成8（获取验证码）
# 扩大到一亿！
# 1->2 96% 完成，前4%用户event_id被设置成26
# 2->3 87.5%完成，剩余


def login_funnel(nums):
    dic = {
        1: 26,
        2: 8,
        3: 18,
        4: 22,
    }
    ids = []
    for i in range(nums):
        for j in range(1, 5):
            ids += [dic[j] for _ in range(5)]
    # index: 5,6,7,8,9,25,26,27,28,29
    for i in range(int(nums*0.04)):
        for j in range(5):
            ids[j+i*20+5]=26


    # index: 50,51,52,53,54, 70,71,...
    for i in range(int(nums*0.125)):
        for j in range(5):
            ids[j + i * 20 + 50] = 88 # 这里目测有点问题
    # index: 20*8+15
    for i in range(int(nums*0.15)):
        for j in range(5):
            ids[j + i * 20 + 175] = 8 # 这里目测也有点问题
    return ids


def group_event(nums):
    groups = []
    for i in range( nums * 4 * 5 // 2):
        groups.append(0)
        groups.append(1)
    return groups


def event_bucket(nums):
    res = []
    for i in range(nums * 4 * 5 // 20):
        for j in range(20):
            res.append(j)
    return res


def generate(start, batch, last_time):
    times, day,last_time = timecal_funnel(last_time, batch)
    data = pd.DataFrame(columns=['user_id', 'event_id', 'time', 'day', 'event_bucket', 'p__carrier'])
    users = user_funnel(batch // 20, start)
    data['user_id'] = users
    data['time'] = times
    data['day'] = day
    ids = login_funnel(batch // 20)
    data['event_id'] = ids
    bucket = event_bucket(batch // 20)
    data['event_bucket'] = bucket
    carry = group_event(batch // 20)
    data['p__carrier'] = carry
    data.to_csv("/data/group7/ourdata/data.csv",header=None index=None,mode='a')
    # data.to_csv("data.csv", index=False, mode='a',header=None)
    return last_time


if __name__ == '__main__':
    batch = 1000000
    num = 100000000
    start = 0
    last_time = "2008-01-01"
    for i in range(num // batch):
        last_time=generate(start, batch, last_time)
        start = start + i * batch
        print(i,"times has been done.")
