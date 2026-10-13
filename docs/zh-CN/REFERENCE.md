# 3. 运行机制

[索引](../../README.md) · [English](../en/REFERENCE.md) · [指令与参数](USAGE.md)

本页解释当前**共享模式**。默认参数和操作指令集中于使用指南；本页不是另一套
操作手册，也不列出过去的测试过程。

## 1. 职责划分

```text
Ozstar login controller
  full-catalogue index → product groups → archive/checksum download → sbatch
                                      ↑                              ↓
                         next batch / bounded continuation ← outcome/state

Offline compute job: independent observation lanes in parallel
  each observation: extract → master preprocess/create → serial target copies
  each target: insert → subtract → extract DS → crop I/V → copy products

Ada login tool: snapshot → incremental pull → SHA256 → ledger + Ozstar receipt
```

一个工作区同时只运行一个共享计算作业；作业内各槽并行处理独立观测组，适当时
每槽执行多轮。登录 controller 必须持续运行才能提交下一批。登录 shell 断开或
controller 停止，不会自动取消已经提交的计算作业。

## 2. 查询、处理资格与数据产品身份

独立查询 catalogue 中的每颗源，包括种子范围外的源。在一度初查之后，执行
公开状态/质量、曝光、大小、排除SB和频率半径筛选。每个 SB 保留最近的合格 beam。
光谱型阈值控制是否处理，不决定是否查询或出现在索引中。

ObsCore `em_min`/`em_max` 是波长上下限，转换为米。取 `c=299792458 m/s`：

```text
frequency_high = c / em_min
frequency_low  = c / em_max
frequency_central = (frequency_high + frequency_low) / 2
radius = (reference_FWHM / 2) × reference_frequency / chosen_frequency
```

半径为 FWHM 全宽的**一半**，频率单位保持一致。central 使用频率的算术中点，
不是 `c / 平均波长`；top 使用频率上限。无效/截断元数据不能静默变成成功的空查询。
没有频率模式标记的历史索引按 top 解释，不会重新标为 central。

共享身份包括 SB、beam、完整 CASDA 文件名、publisher DID、频段上下限。
识别 RACS 的 `SB...beam...` 和 HIPASS 的 `SBxxxxx_Fxx_Bxx`；HIPASS B 映射为
beam，F 仍保留在文件名/身份中。不能仅凭天空距离近就共用模型。

种子范围决定顺序与起始观测；全表中符合资格的伙伴加入这些相同观测，即使位于
范围外，也不递归展开伙伴的其他观测。

## 3. 公共 master 与目标隔离

每个观测在登录节点将**全部 staged URL**（归档和checksum）一次传给
`download_files()`，保留签名 URL 编码。下载后按 tar 成员名确认已准备 MS 数量；
每个共享数据产品必须恰好包含一个 MS，这不是 checksum/内容校验。

计算 worker 使用命令行 tar，保留 checksum，成功解压后删除归档。解压前检查
tar 路径/链接是否指向不安全位置。公共输入位于 `staging/_shared/<model_key>/`：

- `extracted/`：未修改原始 MS。
- `master/`：真实副本，先 preprocess 再 create-model，保留完整未裁剪模型。
- `wsclean_tmp/`：该观测独立成像临时数据。

模型 key 包括产品身份、SIF SHA256 和成像设置。每个目标获得**已经 preprocess 的
master MS**及完整模型的独立副本，不是原始 MS，也不是硬链接。目标的 insert、
subtract、corrected列、掩膜和 cutout 不影响其他目标或公共 master。
master 永远不参与 insert/subtract/crop。

同一观测内目标串行。按每个目标的自行修正坐标裁剪 I/V；历元依次使用 CASDA
`t_min`、旧 CSV、配置的最终后备值。续提可复用 master，不再成像；显式失败重试
则独立重建私有模型。

适用时保留 preprocess 的 `FIELD_OLD` 前置判断。DStools 非零退出会标记目标/步骤
失败，不会无条件继续。文件存在检查点跳过已有指定产物；不增加
`.subtracted.complete`、`.ds.part` 提升或额外 DS/FITS 格式验证关卡。
FITS 裁剪本身仍必然需要读取图像。

## 4. 时间、并行与恢复

归档准备后估算未完成工作，而非仅按 CASDA 行数估算。每个观测计入已准备 MS
建模工作和未完成目标工作；有效的保留 master 可消除建模项。将观测组分配到当前
负载最少的槽。申请时间为最忙槽累计负载加整作业余量，不超过配置上限。容量
不足时拆分观测或目标到后续批次，不会总是申请最大时间。

各槽有独立路径/进程状态。SQLite 使用短事务与锁；大输入复制的空间检查/复制由
`shared-copy.lock` 串行保护，但成像和目标处理仍并行。整作业内存按实际槽数预算，
不是每槽独立硬隔离。WSClean wrapper 补齐缺省线程/内存选项；create-model 为
Python/MS操作保留余量。OpenBLAS 保持单线程，不限制 WSClean 自身的 `-j`。
MS/模型临时数据仍在 FRED。

所有槽共享同一个作业截止时间；接近预留时间时，worker 以75退出并记录部分完成。
登录 controller 有界续提，跳过已成功/失败目标；共享 worker 不自行提交。

完成依据是 manifest、`.worker_finished`/`.worker_failure` 和独立的
`.submission.json` 提交回执，不使用 heartbeat 或 resubmit-pending。如果 worker
被强杀而未留下终态，两次连续明确的 `sacct` 终止状态可记录异常失败；队列为空
或 accounting 查询失败不能证明完成。

独占 controller 锁防止同时拥有同一工作区。锁文件存在不等于锁被占用；删除正在
占用的文件可能绕过保护，却不会释放旧锁。恢复原计划/设置，不猜测并提交新的
重复作业。sbatch 与持久化回执之间中断，会被视为不明确提交，需要检查。

普通观测失败会记录后继续其他目标、槽和后续种子；即使一个源全部观测失败也
继续推进。无法可靠保存状态（例如数据库不可写）时停止，不将其静默当作成功
或可恢复的科学处理结果。

## 5. 状态与清理

| `ASKAP_WORK` 内路径 | 用途 |
| --- | --- |
| `state/crosscatalogue_sources.csv` | 每颗源、处理资格、查询状态/数量/注释 |
| `state/crosscatalogue_observations.csv` | 已选源—观测关联、频率/半径和共享信息 |
| `state/crosscatalogue-summary.json` | 查询分类及关联/组变化数量 |
| `state/crosscatalogue.json` | 已发布机器索引，不要手工修改 |
| `state/crosscatalogue-update.json` | 更新/恢复检查点 |
| `state/crosscatalogue_rows/<revision>/` | 不可变 ECSV 与逐源查询检查点 |
| `state/shared-controller.json`、`state/jobs/` | 冻结计划、manifest、sbatch 和提交/终态信号 |
| `state/sb_processing.sqlite` | 每源/SB/beam一行，步骤、错误、耗时、模型来源和重试计数 |
| `state/sources/`、`logs/` | 源摘要、Slurm日志与保留的失败命令/输出 |

查询状态为 `available`、`no_observations`、`query_failed`、`unknown`。更新失败时
可保留旧行用于检查，但本轮调度不使用失败/未知源的旧结果。CSV注释是列，不在
表头之前插入自由文本。更新可能移除关联或改变最近beam，并非仅追加；不删除
科学产物。活跃计划冻结索引版本，完成前拒绝更新。

步骤标记为 `Y` / `N`，尚未记录为 NULL。失败日志保留命令和输出，常规成功的
详细步骤日志会移除。目标公共 preprocess/create 可以记Y、耗时0，因为只在
master执行一次；公共耗时仍保存在manifest。

输出结构不变：

```text
products/UCSxxxx-yyyy/UCSxxxx/LongObs/SBxxxxx_beamyy/
  SBxxxxx_beamyy.ds
  wsclean_model/wsclean-MFS-I-image.fits
  wsclean_model/wsclean-MFS-V-image.fits
  observation.json
```

完成要求 DS、I、V **文件名存在**且数据产品身份匹配，不代表科学质量合格。
旧产物没有sidecar时，需要SQLite文件名证据；产品冲突时不覆盖。controller完成
表示计划工作已处理，不保证没有失败，也不表示范围外伙伴的其他观测全部完成。

目标三件套成功复制至 products 后才清理其 MS；全部已索引合格伙伴完成后才释放
公共输入/模型。失败/未完成目标保留输入。替换部分目标目录时可能保留
`.previous-*` 备份，不会自动删除。显式重试预算为一次 reset/preprocess/私有重建，
再一次重新下载/处理，之后 `exhausted`，不影响伙伴产物。

## 6. 传输与空间管理

Ada 获取 Ozstar 完整三件套快照，含每个文件的 SHA256/大小。不完整三件套不能
传输。扫描器要求上面恰好四层的产品路径，不能指定额外多一层测试case的父目录。
空索引可以返回PASS且传输零文件，不证明科学处理成功。不要同时向同一Ada产品树/
台账拉取。不同源项目需要隔离时，使用独立Ada子目录/台账。

只有缺失/hash不符文件进入rsync清单。`--ignore-times` 即使大小/mtime相同也强制
修复这些已选文件，健康文件不进入清单。拉取后检查本地hash及源稳定性。正常流程
只有三个Y和验证过的上传回执齐全时，Ada台账才记complete。重复拉取更新审计历史，
不新增重复观测行。

Ada 在配置根目录保留 `state/transfer_ledger.sqlite`、manifest和不可变回执，
将验证回执上传至选定Ozstar工作区。这与处理数据库不同。只传DS/I/V，不传
provenance sidecar、原始MS或完整模型；Ozstar的来源记录/状态应保留，用于恢复
处理与科学可追溯性。

没有自动的传输后删除功能。手动清理前确认精确源/SB/beam、全部三个验证文件与
匹配回执，并保留适当科学/状态备份。不能仅凭controller PASS清空products或全部staging。
