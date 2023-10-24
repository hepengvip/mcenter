# Protocol of mcenter

## 设置UserId

客户端连接后，必须设置userId作为用户唯一标识。

在设置UserId前，无法做其他操作；后续可能会扩展成认证+授权模式。

命令格式：

```shell
USER [req_id] <userId>\n
```

参数说明：

- userId 用户标识

返回说明：

- 0: ok
- 100001: user exists

## 频道管理

目前只支持频道创建。命令格式：

```shell
CHAN [req_id] <channelName>\n
```

参数说明：

- channelName 频道名称

返回说明：

- 0: ok

## 订阅管理

使用`SUB`订阅一个频道。
使用`UNSUB`取消订阅一个频道。
命令格式：

```shell
SUB [req_id] <channelName>\n
UBSUB [req_id] <channelName>\n
```

参数说明：

- channelName 频道名称

返回说明：

- 0: ok

## 发布消息

发布消息到一个频道。命令格式：

```shell
PUB [req_id] <channelName> <messageSize>\n<message>
```

参数说明：

- channelName 频道名称
- message 消息体

返回说明：

- 0: ok

## 响应

```shell
REP [req_id] <response_code:response_message>\n
```

## 接收订阅的消息

```shell
MSG <userId> <channelName> <messageSize>\n<message>
```
