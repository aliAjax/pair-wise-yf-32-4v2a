# 器官分配与转运协调系统

Python 标准库独立项目。系统按器官类型、血型、地域、医疗匹配、紧急程度和等待时间排序候选患者，并管理提出、接受、转运、交接、植入或撤回流程。器官过期后所有继续流转操作都会被阻止，全部状态变化写入审计记录。手术间与麻醉团队按医院、按日限量：提出分配时占位，容量满后排队，撤回或器官过期释放并按先到先得递补。

## 运行

```bash
python3 app.py --db organ_allocation.db
```

默认监听 `127.0.0.1:8203`，首页 `/`，健康检查 `/health`。

身份头：`X-User-Id`、`X-Role`。角色为 `viewer`、`hospital`、`coordinator`、`allocation_officer`、`auditor`；医院角色还需 `X-Hospital`。

## 主要接口

- `POST /api/donors`、`POST /api/candidates`：登记器官与候选患者。
- `GET /api/donors/{id}/ranking`：查看兼容候选排序。
- `POST /api/slots`、`GET /api/slots`：医院登记/更新某日手术间与麻醉团队台数，协调台查看占用、剩余与排队队列。
- `POST /api/allocations`：提出唯一分配。可带 `slot_date`（默认 UTC 今天）和幂等键 `client_token`。
- `POST /api/allocations/{id}/accept`、`withdraw`：医院确认或撤回。
- `POST /api/allocations/{id}/transit`、`delay`：冷链转运和延误上报。
- `POST /api/allocations/{id}/handoff`、`handoff-accept`：来源医院发起、接收医院确认。
- `POST /api/allocations/{id}/implant`：确认植入。
- `GET /api/allocations/{id}/audit`、`GET /api/state`：完整审计和权限视图（`state` 含 `slots`）。

## 手术间容量与排队规则

- 医院按日登记 `room_count` 和可选 `anesthesia_team_count`，两者均给出时有效容量取最小值；同一医院同一天重复登记为更新，容量不能调减到低于当日已占用数。
- 提出分配时按候选患者医院和 `slot_date` 占用一台（`slot.status=held`）；容量满则进入 FIFO 队列（`queued`），响应与分配单视图带 `occupied`、`remaining`、`queue_position`。
- 并发提出由数据库写事务串行化，同时提交只有先到者拿到最后一间，后到者排队并看到剩余台数。
- 医院撤回或器官过期（含排队中懒过期）立即释放占位，并自动把当日队列最前面的有效分配单递补为占位；仍在排队的单不能被接受，会返回 `slot_pending` 与剩余台数。
- 分配单入库与占位分两个事务提交。入库失败时占位保留；使用相同 `client_token` 重试会复用原占位而不重复占台，重复提交返回同一张分配单。
- 未登记容量的医院按旧流程运行，不阻塞分配。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

血型兼容与评分是演示规则，不包含 HLA 分型、器官大小、病程、儿科差异和真实移植网络规则。医院身份使用请求头模拟，SQLite 环境适合原型，不处理跨机构身份信任、远程患者隐私协议和真实冷链设备接入。
