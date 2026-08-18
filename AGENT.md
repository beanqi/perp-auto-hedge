# AGENT.md

本文件用于指导本项目的所有开发工作。

核心目标只有一个：

> **在正确实现当前需求的前提下，让代码尽可能简单，并持续控制代码熵增。**

项目不是为了展示架构能力，也不是为了为未知未来预留扩展点。

代码应该做到：

```text
Simple > Clever
Concrete > Abstract
Explicit > Magical
Current Requirement > Future Flexibility
Small Diff > Architecture Rewrite
Delete > Add
```

如果两个方案都能正确解决问题，永远选择：

```text
更少的代码
更少的类型
更少的模块
更少的状态
更少的依赖
更少的间接层
更容易直接读懂的那个
```

---

# 1. 开发前

修改代码前必须先理解当前需求和已有设计。

优先阅读：

```text
docs/业务设计.md
docs/架构设计.md
```

不要在没有理解现有数据流和状态机的情况下直接增加新模块。

当需求可以在现有结构内完成时：

> **不要修改架构。**

当架构确实无法表达当前需求时，只做满足当前需求所需的最小调整。

不要因为：

```text
以后可能会用
以后可能会扩展
以后可能有更多交易所
以后可能有更多策略
以后可能切换实现
```

而提前设计当前不需要的能力。

未来需求出现时，再为真实需求重构。

---

# 2. 控制代码熵

每增加一个概念，项目的理解成本都会增加。

新增以下任何东西之前都必须先问：

```text
真的需要这个模块吗？
真的需要这个类型吗？
真的需要这个 trait 吗？
真的需要这个配置吗？
真的需要这个状态吗？
真的需要这个 wrapper 吗？
真的需要这一层转发吗？
```

如果答案只是“以后可能有用”，不要加。

## 2.1 优先复用现有概念

优先：

```text
给现有结构增加一个明确字段
给现有 enum 增加一个明确 variant
给现有模块增加一个小函数
```

而不是：

```text
创建新 abstraction
创建新 manager
创建新 service
创建新 framework
创建新中间层
```

## 2.2 一个事实只有一个来源

同一个状态或事实不要在多个地方重复维护。

例如：

```text
PairState        -> Engine 唯一修改
Market Snapshot  -> MarketStore 唯一维护
Execution 状态   -> ExecutionContext 唯一维护
真实仓位         -> Exchange Position 为最终事实
```

不要为了调用方便复制一份长期状态到其他模块。

复制状态会产生同步问题，最终增加隐式复杂度。

## 2.3 删除优先

修改旧代码时主动寻找可以删除的内容：

```text
失效分支
重复逻辑
无用字段
无用类型
过期注释
不再需要的配置
仅做转发的 wrapper
```

一个功能完成后，如果代码量可以减少而行为不变，应优先减少。

---

# 3. 抽象规则

抽象必须解决当前已经存在的重复或边界问题。

不要为了“设计漂亮”抽象。

## 3.1 不要过早创建 trait

只有满足以下至少一个条件时才考虑 trait：

```text
当前已经存在多个真实实现
需要隔离明确的外部边界
测试确实需要替换该边界
```

不要因为“未来可能有第二个实现”就创建 trait。

Exchange 这类天然存在多个交易所实现的边界可以抽象，但抽象只统一系统真正需要的语义，不强行抹平交易所差异。

## 3.2 不创建无意义的层

避免出现只有转发作用的结构：

```text
XxxManager
XxxService
XxxHandler
XxxProcessor
XxxCoordinator
XxxFactory
```

这些名字不是绝对禁止，但如果一个类型不能表达明确领域职责，就不应该存在。

优先使用业务名称：

```text
Engine
Strategy
Risk
Execution
MarketStore
PairRuntime
OrderBook
```

## 3.3 不建立通用框架

当前项目禁止为了少量场景引入：

```text
通用事件总线
通用状态机框架
通用工作流框架
插件系统
依赖注入框架
复杂 Repository 层
复杂 DDD 分层
自定义 ORM 式抽象
通用规则引擎
```

直接写清楚当前业务流程通常更好。

---

# 4. 模块边界

保持当前架构中的职责边界：

```text
Exchange
    处理交易所协议与外部事实

MarketStore
    保存最新市场事实

Strategy
    计算交易价值

Risk
    判断是否允许承担风险

Execution
    执行已经确定的交易计划

Engine
    编排流程，并且是 PairState 唯一修改者

Storage
    持久化不能安全丢失的数据
```

不要让模块跨边界做“顺手”的事情。

例如：

```text
Strategy 不修改 PairState
Risk 不发送订单
Execution 不重新计算策略
Exchange 不判断是否开仓
Storage 不承载业务决策
```

边界越明确，需要记住的隐式规则越少。

---

# 5. Exchange 规则

Spot 和 Perp 是两个真实不同的市场接口，不为了代码复用强行合并实现。

保持：

```text
exchange/<exchange>/spot.rs
exchange/<exchange>/perp.rs
```

统一的是系统内部真正需要的数据结构和调用语义，不是交易所原生 API。

正常交易链路：

```text
Market WebSocket  -> 行情
Private WebSocket -> Order / Fill / Position Update
Trading WebSocket -> Place / Cancel Order
```

下单和撤单使用 Trading WebSocket。

REST 主要用于：

```text
启动
查询
Reconcile
异常恢复
```

不要维护两套正常下单路径。

---

# 6. OrderBook 规则

系统内部永远使用多档 OrderBook：

```text
OrderBook {
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
    ...
}
```

当前虽然只接 L1 实时深度：

```text
bids = [best_bid]
asks = [best_ask]
```

但所有 VWAP、流动性检查和本地撮合都按照多档逐档消费。

不要为 L1 单独写一套业务逻辑。

未来切换 L2：

```text
Snapshot + Incremental Update
→ Exchange adapter 维护多档 OrderBook
→ 下游逻辑不变
```

深度不足必须显式返回失败：

```text
available_quantity < target_quantity
→ DepthInsufficient
```

绝对不要把最后一档价格外推到不可见数量。

---

# 7. 状态机规则

状态必须少而明确。

当前 PairState：

```text
WARMING_UP
WATCHING
OPENING
HOLDING
CLOSING
RECOVERING
HALTED
```

不要把每一种业务条件变成状态。

例如：

```text
liquidity_ok = false
regime_ok = false
borrow_ok = false
```

应该是普通条件或 `blocked_reason`，而不是新增：

```text
LIQUIDITY_BLOCKED
REGIME_BLOCKED
BORROW_BLOCKED
```

状态转换直接使用显式 `match`。

不要引入状态机框架。

PairState 只能由 Engine 修改。

---

# 8. Rust 实现规则

## 8.1 函数只做一件事

函数应该能用一句简单的话描述职责。

如果一个函数同时：

```text
查询
计算
修改多个状态
发送请求
持久化
```

通常说明职责过多。

但也不要为了追求短函数，把连续的简单逻辑拆成大量只能跳转阅读的小函数。

目标不是“函数越短越好”，而是：

> **控制流一眼可以看懂。**

## 8.2 优先显式数据结构

优先：

```rust
struct

enum

match

Result

Option
```

少使用需要读者猜测行为的技巧。

领域状态优先使用 enum，而不是字符串、整数或多个 bool 拼接表达。

## 8.3 降低嵌套

优先使用：

```text
early return
match
let-else
明确的 guard
```

避免多层 `if -> if -> match -> if`。

当控制流已经难以在一个屏幕内理解时，先简化逻辑，而不是增加注释解释复杂逻辑。

## 8.4 错误必须有业务意义

不要吞掉错误。

不要把所有错误都包装成巨大通用错误体系。

只保留当前调用方真正会处理的错误分类，例如：

```text
DepthInsufficient
OrderRejected
TradingWsUnavailable
ExecutionImbalance
```

外部输入、网络、交易所响应相关的运行路径避免使用 `unwrap()` / `expect()`。

对于程序初始化阶段确实不可能继续运行的错误，可以直接失败，不必设计复杂恢复框架。

## 8.5 Clone 要有意识

不要为了绕过所有权问题随手 `.clone()` 大对象或运行时状态。

如果 clone 只是为了让代码更容易写，先检查数据所有权是不是设计得太复杂。

但也不要为了消灭一个廉价 clone 引入生命周期体操或复杂引用关系。

简单优先。

---

# 9. 并发与异步

并发是高熵来源。

只有真实需要并发的 I/O 才并发。

不要为了“事件驱动”把每个模块都变成独立 task。

优先：

```text
一个明确 owner
一个明确事件入口
一个明确状态修改点
```

避免：

```text
多个 task 同时修改同一状态
到处共享 Arc<Mutex<_>>
层层 channel 转发
没有生命周期管理的 detached task
```

如果直接函数调用能解决问题，就不要增加 channel。

如果一个 channel 只是把 A 的调用原样转发给 B，它很可能不需要存在。

---

# 10. 依赖管理

每增加一个 crate 都会增加：

```text
API surface
升级成本
编译成本
安全风险
理解成本
```

添加依赖前先确认标准库或当前依赖不能简单解决。

但不要为了避免一个成熟且合适的小依赖，自己实现大量复杂基础设施。

判断标准仍然是：

> 哪个方案让整个项目的总复杂度更低。

禁止为了未来可能使用的功能提前添加依赖。

---

# 11. 配置

配置项不是免费的。

每增加一个配置，就增加一个长期维护的行为分支。

只有当前确实需要运行时调整的值才进入配置。

不要把所有常量都配置化。

不要为了“灵活”增加：

```text
多层配置覆盖
动态插件配置
复杂 feature toggle
当前只有一个取值的 enum 配置
```

能写死且属于当前明确业务规则的内容，可以先写死。

---

# 12. 日志

日志用于回答真实运行问题，不用于记录程序每一步。

重点记录：

```text
状态转换
交易请求
订单 Ack / Reject
Fill
ExecutionImbalance
Reconcile
关键风险事件
WebSocket 连接状态变化
```

避免在高频行情主链路打印无意义日志。

日志字段应包含可以关联交易的 ID：

```text
pair_id
trade_id
execution_id
order_id
market_id
```

---

# 13. 测试

测试优先覆盖资金和状态风险最高的地方：

```text
OrderBook 多档撮合
DepthInsufficient
PairState 转换
部分成交
双腿数量不匹配
ExecutionImbalance
Restart / Reconcile
```

测试业务行为，不测试实现细节。

一个简单函数如果已经显而易见，不需要为了覆盖率制造大量低价值测试。

不要为了测试引入比生产代码更复杂的 mocking framework。

---

# 14. 修改代码时的默认流程

每次任务按照下面的顺序进行：

```text
1. 明确当前需求
2. 阅读相关现有代码和文档
3. 找到最小修改点
4. 优先修改现有结构
5. 只有必要时才增加新概念
6. 实现
7. cargo fmt
8. cargo check
9. cargo test
10. 做一次代码熵检查
```

代码熵检查必须问：

```text
有没有可以删除的代码？
有没有可以合并的类型？
有没有只做转发的层？
有没有为了未来增加的抽象？
有没有重复维护的状态？
有没有不必要的配置？
有没有不必要的依赖？
有没有可以直接写清楚却被抽象隐藏的逻辑？
```

只要答案是“有”，继续简化后再结束任务。

---

# 15. 修改范围

默认选择 Small Diff。

一个功能修改不应该顺便：

```text
重命名大量无关代码
重新组织整个目录
统一所有历史风格
替换无关依赖
重构无关模块
```

除非这些改动是完成当前任务所必需的。

不要把“顺手重构”混入业务修改。

真正需要重构时，也应该单独、明确、最小化。

---

# 16. 注释与文档

代码应该优先通过结构和命名表达意图。

注释主要解释：

```text
为什么这样做
交易所特殊约束
业务上不明显的规则
不能被破坏的 invariant
```

不要注释代码字面上已经表达的事情。

错误：

```rust
// increment retry count
retry_count += 1;
```

有价值：

```text
// WebSocket 重连后必须先 Reconcile，避免把断线期间已成功的订单再次发送。
```

架构发生真实变化时同步修改 `docs/架构设计.md`。

业务规则发生真实变化时同步修改 `docs/业务设计.md`。

不要让代码和文档描述两套系统。

---

# 17. 最终原则

本项目不追求：

```text
最通用
最灵活
最抽象
最可配置
最像框架
```

本项目追求：

```text
正确
简单
直接
稳定
容易理解
容易删除
容易修改
```

任何设计如果需要大量解释才能证明它是合理的，通常已经太复杂。

任何抽象如果当前只有一个调用场景，优先直接实现。

任何状态如果可以从已有状态推导，优先不要存。

任何模块如果没有清晰独立职责，优先不要建。

任何代码如果删掉后仍然正确，删掉。

最后始终记住：

> **代码是负债，不是资产。**
>
> **解决问题所需的最少代码，通常就是最好的代码。**
>
> **持续控制熵增，比一次设计出“完美架构”更重要。**
