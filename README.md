

## 获取 Raft 领导节点

```bash
curl -X GET http://localhost:9090/raft/leader

```

## 添加 raft 节点

```bash
curl -X POST http://localhost:9090/raft/add_node -H "Content-Type: application/json" -d '{"node": "0:172.19.29.110"}'
```

## 移除 raft 节点

```bash
curl -X POST http://localhost:9090/raft/remove_node -H "Content-Type: application/json" -d '{"node": "1:172.19.29.122"}'
```

## 恢复异步任务

```bash
curl http://localhost:9090/stats/recovery_tasks
```


