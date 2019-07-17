生成模拟数据需对项目中三个功能函数进行测试。针对不同的功能需要的数据维度不同，具体如下：

day,time,user_id,event_id, p_utm_source,event_bucket

event_id: 26,8,18,22,27->完成项目创建
event_bucket:0-19

## 漏斗分析

漏斗分析中需要的数据属性包括：time, event_id, day, user_id

根据漏斗分析的特点，我们设计了三套测试流程，生成数据进行测试，具体如下 ：

1. 点击注册 event_id=26 
2. 注册-获取验证码 event_id=8
3. 注册-输入验证码 event_id=18
4. 完成注册 event_id=22
> 好像只写完了👆🏿👆这个第一套流程，害没关系剩下的都一样！嗯！

1. 点击忘记密码 event_id=5
2. 找回密码-获取验证码 event_id=19
3. 找回密码-重置密码 event_id=28
4. 提交新密码 event_id=1

1. 创建项目-选择项目模版 event_id=16
2. 创建项目-添加团队成员 event_id=12
3. 创建项目-添加客户 event_id=15
4. 完成项目创建 event_id=27

## 事件分析

事件分析中需要的数据属性包括：time, event_id, event_feature, day, user_id

可以查完成注册的人，可以直接用漏斗分析的生成数据进行测试


 
## 留存分析

留存分析中需要的数据属性包括：time, day, event_id, user_id

根据留存分析的特点，我们设计了一下测试流程，生成数据进行测试，具体如下：

1. 初始行为：点击注册 event_id=26
2. 后续行为：完成注册 event_id=22
