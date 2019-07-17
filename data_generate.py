import pandas as pd
import time
import datetime




def timecal_funnel(t,num):  # t-> "2008-01-01"
    t += " 00:00:00"
    # 20分钟生成一个时间戳
    from_time = int(time.mktime(time.strptime(t, "%Y-%m-%d %H:%M:%S")))
    times = []
    days = []
    for i in range(num):
        times.append(from_time + i * 20 * 60)
    for x in times:
        days.append(x // 86400)
    for i in range(len(times)):
        times[i] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(times[i]))
        times[i] += ".000000000"
    return times, days


# 每名用户隔5条又出现一次
def user_funnel(num):
    users = []
    for i in range(num):
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
    for j in range(5):
        ids[j + 5 * 1] = 26
        ids[j + 5 * 5] = 26
    # index: 50,51,52,53,54, 70,71,...
    for i in range(6):
        for j in range(5):
            ids[j + i * 20 + 50] = 88
    # index: 20*8+15
    for i in range(6):
        for j in range(5):
            ids[j + i * 20 + 175] = 8
    return ids


def group_event(nums):
    groups=[]
    for i in range(nums*4*5//2):
        groups.append(0)
        groups.append(1)
    return groups
def event_bucket(nums):
    res=[]
    for i in range(nums*4*5//20):
        for j in range(20):
            res.append(j)
    return res


if __name__ == '__main__':
    times, day = timecal_funnel("2008-01-01",100000000)
    data = pd.DataFrame(columns=['user_id', 'event_id', 'time', 'day', 'event_bucket', 'p__carrier'])
    users = user_funnel(5000000)
    data['user_id'] = users
    data['time'] = times
    data['day'] = day
    ids = login_funnel(5000000)
    data['event_id'] = ids
    bucket=event_bucket(5000000)
    data['event_id']=bucket
    carry=group_event(5000000)
    data['p__carrier']=carry
    data.to_csv("data.csv",index=False)


