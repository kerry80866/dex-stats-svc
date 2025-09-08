

## 获取 Raft 领导节点

```bash
curl -X GET http://localhost:9090/raft/leader

```

## 添加 raft 节点

1. leader 添加节点

```bash
curl -X POST http://localhost:9090/raft/add_node -H "Content-Type: application/json" -d '{"node": "0:192.168.110.236"}'
```

2. 新节点 raft 配置中的 join 设置为 true

## 移除 raft 节点

```bash
curl -X POST http://localhost:9090/raft/remove_node -H "Content-Type: application/json" -d '{"node": "1:172.19.29.122"}'
```


