# Ozstar ASKAP UCD Pipeline

Process ASKAP visibility data on Ozstar and retrieve verified DS/Stokes-I/V
products from HPC-ada. The pipeline uses a full-catalogue observation index
and shared models; the default radius uses central frequency.

## English documentation

1. [Installation](docs/en/INSTALLATION.md) — prerequisites, deployment, Python, SIF and credentials.
2. [Usage](docs/en/USAGE.md) — minimal commands, optional parameters, indexing, processing, retries and Ada retrieval.
3. [How it works](docs/en/REFERENCE.md) — selection, model isolation, scheduling, recovery, state and cleanup.
4. [SIF build (optional)](docs/en/SIF_BUILD.md) — rebuild the container only when needed.

Repository layout: `ozstar_askap/` contains the pipeline and transfer tools;
`New_dstools_build/` contains the offline SIF recipe and patches;
`Download_ASKAP_Visibility_Remote.py` is the standalone single-position downloader.
Deployment code and runtime data directories are configured separately. Do not
commit passwords, MS/SIF files, products or runtime state. Historical validation
material and regression code are retained, but are not part of the daily runbook.

## 中文说明

在 Ozstar 处理 ASKAP visibility 数据，由 HPC-ada 拉取并校验 DS/Stokes-I/V
产物。程序使用全表观测索引与共享模型，交叉匹配半径默认按中心频率计算。

1. [安装](docs/zh-CN/INSTALLATION.md) — 环境要求、部署、Python、SIF 与凭据。
2. [使用](docs/zh-CN/USAGE.md) — 最简指令、可选参数、索引、处理、重试和 Ada 拉取。
3. [运行机制](docs/zh-CN/REFERENCE.md) — 筛选、模型隔离、调度、恢复、状态和清理。
4. [SIF 构建（可选）](docs/zh-CN/SIF_BUILD.md) — 仅在需要时重建容器。

仓库结构：`ozstar_askap/` 包含处理程序与传输工具；`New_dstools_build/` 包含
离线 SIF 构建配方与补丁；`Download_ASKAP_Visibility_Remote.py` 用于单个坐标的
独立下载。程序部署目录与运行数据目录分别配置。不要提交密码、MS/SIF、产物或
运行状态。历史验证资料和回归代码继续保留，但不属于日常操作手册。
