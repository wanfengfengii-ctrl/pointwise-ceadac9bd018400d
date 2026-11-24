# 两地日账换版

一轮只办一个观测日的 A→B。启用时两站已有同一冻结 A 的各自原回执，运营方指定同日的确切冻结目标 B。站点标识指接收库，与日表内的段名无关。接收库及其复制执行库沿 README 的方式打开。

## 调用约定

- `publish.service.configure_switch(receiver, switch_id, site, peer_site, old_receipt_id, target_manifest)` 在各站分别登记，返回 `None`。`old_receipt_id` 是本站原签 A，`target_manifest` 是 V1 的 `Manifest`。登记后重开仍用同一配置。同号同配置重投无副作用，改绑报 `Conflict`。两站使用相同切换号，填写互为对站的标识。站名和切换号沿用非空 UTF-8 标签约定。
- `publish.service.prepare_switch(receiver, port, switch_id, request_id)` 按本站复制请求办理备切，成功返回 `(switch_id, site, receipt)`，其中 `receipt` 是本站正常签发的 B 回执。普通流程已签的 B 可以沿用原回执。缺段报 `NotReady`，错版拒绝。备妥以本站实际提交的完整签发为依据，同一次备妥重投返回原结果，已备妥后改绑别的本地请求报 `Conflict`。单独调用普通签发不会取得当前供 B 的资格。
- `publish.service.advance_switch(receiver, switch_id, send=None)` 从本站办理或续办换版。待办报 `NotReady`。成功返回 `(switch_id, target_manifest, bindings)`，`bindings` 为按站名升序排列的 `((site, receipt_id), ...)`。两站成功返回的是同一个结果。本站已经成功返回时，本站已持久确认，可不连对站重领同一个结果并独立供 B。两地备妥后恢复正常送达，分别续办应能完成确认。只从本站已提交的目录取得本站证明，按站点和确切清单核对对站证明。等额、同编号或把一站证明当成两站证明均不足以换版。
- `audit.readback.read_current(receiver, site, day, send=None)` 读本站当前账，成功返回原 `ReadbackResult`，待切换报 `NotReady`。连通但尚欠一地有效备妥时读 A。任一地首次读出 B 后，从另一地随后发起的当前查询只能读 B 或报 `NotReady`。本站持久确认后重开，`send=None` 也能读出本站 B 的原回执、原序明细、出处和合计。原 `read` 按号领 A 或已签 B 的行为照常。

## 消息边界

`send(peer_site, body)` 只向指定对站传 `bytes` 并返回 `bytes`。收件入口为 `publish.service.receive_switch(receiver, from_site, body)`。传输层在消息体之外给定可信的实际发站，消息内容由交付实现定义。`send=None` 视为断链。`ConnectionError` 表示本轮联络没有可用答复，消息可能尚未送达，也可能已生效而回包失落。旧消息可在新消息之后重投。两库各自提交，只能通过此接口交流。收件方不可把错站、错单或错版作为有效依据。单库提交失败不产生该次的新资格或成功结果，另一库此前已经成功提交的事实仍有效。恢复送达后继续使用原切换号。源库和复制执行库在两个本地签发完成后可关闭。

## 样例与运行

`python3 -B switch_fixture.py` 核对夹具的原有复制、签发和按号领取链路。`sample_pair(root)` 给出站名为 `east`、`west` 的两库、各自端口、`initial` 的两份 A 回执和 `target` 清单。源库在夹具交出时已关闭。A 的原序段量为 5、7 L。B 为 11、13 L，东库已到齐，西库只到首段。西库另有已到齐的请求 C，值为 10、14 L，与东库 B 的修订号、清单号及总量相同。可先用普通入口签 C，检查同一回执号也不能代替真 B。用 `finish(receiver, port, 'B', slice(1, None))` 给西库送达末段。`receiver_paths` 可供关库后重开。

`Link.handlers[site]` 可登记该站收件回调，`Link.sender(site)` 提供本站发送函数。`intercept(stage, frame)` 可在 `before` 或 `after` 抛出 `ConnectionError`，分别表示尚未送达或送达后失答。`Link.frames` 留下已尝试传递的帧，`deliver(frame)` 可重投。`fail_next_commit(receiver)` 可让指定连接的下一次显式提交失败。测试联络和数据库失败后均从已提交状态续办。

用根目录 `.pointwise-runtime.json` 接入新增业务核验与演示，保留原核验和原演示入口可运行。清单沿用现有 v1 CLI 格式，`setup` 可空，`verify` 和 `demo` 分别调用真实核验和演示命令。核验欠段及等额错版时仍读 A，控制切换消息到达并在一次成功读 B 后从对站发起新读。核验提交失败、送达后失答、消息重投和同号续办。两地各自确认后，断开所有对端并分别重开，当前查询读出 B 的 24 L 和原出处，按旧回执仍读出 A 的 12 L。
