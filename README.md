# HBase 智能诊断 Agent

基于 AI 的 HBase 集群智能诊断工具，自动分析日志和指标数据，快速定位集群问题根因。

## ✨ 核心功能

- 🤖 **AI 智能诊断**: 基于Bedrock，自动分析 HBase 问题
- 📊 **日志分析**: 支持 14 个专业领域的深度日志解析
- � **指标分析**: 性能指标异常检测和趋势分析
- 🎯 **根因定位**: 提供 Top 3 根因分析和解决建议

## � 快装速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置 AWS 凭证
```bash
aws configure  # 配置 Bedrock 访问权限
```

### 3. 准备数据

#### 方式一：使用 S3 日志下载脚本（推荐）
```bash
# 从 S3 自动下载AWS EMR HBase 日志
python s3_log_downloader.py bucket-name cluster-id "2025-08-22 06:00:00" "2025-08-22 18:00:00"

# 示例
python s3_log_downloader.py aws-logs j-1X4RY58NQKZ1 \
  "2025-08-22 06:00:00" "2025-08-22 18:00:00"
```

脚本特性：
- 🚀 **智能节点识别**: 自动从 EMR API 获取集群节点信息
- 🎯 **精确日志匹配**: 根据节点角色下载对应日志（Master/RegionServer）
- ⚡ **高效下载**: 直接构建路径，无需扫描整个 S3
- 📊 **实时进度**: 显示下载进度和成功/失败统计

#### 方式二：手动准备
将 HBase 日志文件放入 `hbase_log/` 目录，指标数据放入 `hbase_metrics/` 目录

## 🤖 使用 AI 诊断 Agent

### 启动 Agent
```bash
python hbase_agent.py --interactive
```

### 与 Agent 对话
启动后，直接输入你的问题，Agent 会自动分析并给出诊断报告：

**示例对话：**
```
用户: 9月12号hbase集群handler满了，帮我找出来top3的原因

Agent: 🔍 开始分析HBase日志...
       📊 发现 25 个节点，处理了 1,092,034 条日志记录
       🚨 检测到 7 个异常，1 个性能问题
       
       # HBase集群Handler饱和问题诊断报告
       
       ## 🎯 TOP 3 根本原因分析
       
       ### 🥇 根因1: 大规模客户端连接风暴
       **技术证据**: 668,893个客户端连接事件
       **影响分析**: 连接风暴导致handler池快速耗尽
       
       ### 🥈 根因2: WAL写入性能瓶颈
       **技术证据**: 190,188次WAL慢同步，平均332.2ms
       **影响分析**: WAL延迟导致handler长时间占用
       
       ### 🥉 根因3: 超时级联效应
       **技术证据**: 668,870个timeout错误，错误率61.2%
       **影响分析**: 超时重试形成恶性循环
```

### 常见问题类型
- `集群响应慢，帮我分析原因`
- `某个节点频繁GC，什么问题？`
- `WAL同步慢，影响写入性能`
- `Region分裂异常，需要排查`

## 📥 S3 日志下载工具

### 基本用法
```bash
python s3_log_downloader.py <bucket> <cluster-id> <start-time> <end-time> [--output dir]
```

### 参数说明
- `bucket`: S3 存储桶名称
- `cluster-id`: EMR 集群 ID（如：j-1X4RY58NQKZ1）
- `start-time`: 开始时间（格式："2025-08-22 06:00:00"）
- `end-time`: 结束时间（格式："2025-08-22 18:00:00"）
- `--output`: 输出目录（默认：hbase_log）

### 使用示例
```bash
# 下载指定时间段的日志
python s3_log_downloader.py aws-logs j-1X4RY58NQKZ1 \
  "2025-08-22 06:00:00" "2025-08-22 18:00:00"

# 指定输出目录
python s3_log_downloader.py aws-logs j-1X4RY58NQKZ1 \
  "2025-08-22 06:00:00" "2025-08-22 18:00:00" --output logs_0822

# 下载全天日志
python s3_log_downloader.py aws-logs j-1X4RY58NQKZ1 \
  "2025-08-22 00:00:00" "2025-08-22 23:59:59"
```

### 工作原理
1. **EMR API 集成**: 调用 `describe_cluster` 和 `list_instances` 获取节点信息
2. **智能路径生成**: 根据节点角色生成对应的日志文件路径
   - Master 节点：`hbase-hbase-master-{ip}.log.{time}.gz`
   - Core 节点：`hbase-hbase-regionserver-{ip}.log.{time}.gz`
3. **批量下载**: 直接下载，失败文件自动记录

### 输出示例
```
集群: j-1X4RY58NQKZ1
时间: 2025-08-22 06:00:00 ~ 2025-08-22 18:00:00
从EMR服务获取集群信息: j-1X4RY58NQKZ1
集群名称: hbase-ai
集群状态: TERMINATED
节点信息:
  i-0c24ce180ec2a4ac2 -> 192.168.15.61 (ip-192-168-15-61) [CORE]
  i-074decf44c5140859 -> 192.168.15.203 (ip-192-168-15-203) [CORE]
  i-0050942c8b5881a3c -> 192.168.12.32 (ip-192-168-12-32) [CORE]
  i-0d50912d0b0972871 -> 192.168.18.209 (ip-192-168-18-209) [MASTER]

生成路径详情:
  时间范围: 2025-08-22 06:00:00 ~ 2025-08-22 18:00:00
  小时数: 13
  节点数: 4
  生成路径数: 52
  Master日志: 13 个
  RegionServer日志: 39 个

下载完成!
成功: 39 个文件
失败: 13 个文件
```
