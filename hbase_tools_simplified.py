#!/usr/bin/env python3
"""
HBase数据清洗工具 - 简化版
纯数据清洗和分析工具，不提供任何推荐或建议
"""
import json
import os
import re
import gzip
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import statistics
from typing import Dict, List, Any, Optional
from strands import tool

@tool
def analyze_hbase_logs(
    log_dir: str = "hbase_log",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    focus_areas: Optional[List[str]] = None,
    target_nodes: Optional[List[str]] = None
) -> dict:
    """
    清洗和解析HBase日志文件数据。纯数据处理，不提供推荐或建议。
    
    Args:
        log_dir: HBase日志目录路径
        start_time: 分析开始时间 (格式: "2025-09-12 16:00:00")
        end_time: 分析结束时间 (格式: "2025-09-12 18:00:00")
        focus_areas: 关注领域 (可选，如: ["handler", "wal", "gc", "memory", "compaction", "split", "flush", "queue", "network", "table", "balancer", "errors", "clients", "performance"])
        target_nodes: 目标节点列表 (可选，如: ["ip-10-25-130-219"])
        
    Returns:
        dict: 清洗后的日志数据，包含事件统计、错误统计、性能数据等
    """
    print("🔍 开始分析HBase日志...")
    print(f"📂 日志目录: {log_dir}")
    if target_nodes:
        print(f"🎯 目标节点: {target_nodes}")
    if focus_areas:
        print(f"🔍 关注领域: {focus_areas}")
    
    # 1. 发现日志文件
    print("\n📁 步骤1: 发现日志文件...")
    log_files = _discover_log_files(log_dir)
    print(f"   ✓ 发现 {len(log_files)} 个日志文件")
    
    # 2. 确定时间窗口
    print("\n⏰ 步骤2: 解析时间窗口...")
    time_window = _parse_time_window(start_time, end_time)
    duration = time_window['end'] - time_window['start']
    print(f"   ✓ 分析时间窗口: {time_window['start_str']} - {time_window['end_str']}")
    print(f"   ✓ 时间跨度: {duration}")
    
    # 3. 过滤相关文件
    print("\n� 步骤 3: 过滤相关文件...")
    relevant_files = _filter_relevant_files(log_files, time_window, target_nodes)
    print(f"   ✓ 筛选出 {len(relevant_files)} 个相关文件 (从 {len(log_files)} 个文件中)")
    
    # 4. 解析日志
    print("\n📝 步骤4: 解析日志内容...")
    log_data = _parse_log_files(relevant_files, time_window, focus_areas)
    print(f"   ✓ 解析了 {log_data['total_entries']} 条日志记录")
    print(f"   ✓ 涉及节点: {log_data['nodes']} ({len(log_data['nodes'])} 个)")
    
    # 5. 数据统计和分类
    print("\n📊 步骤5: 数据统计和分类...")
    analysis_result = _analyze_parsed_logs(log_data)
    print(f"   ✓ 统计到 {len(analysis_result['anomalies'])} 个数据异常")
    print(f"   ✓ 识别到 {len(analysis_result['performance_issues'])} 个性能数据")
    print(f"   ✓ 记录 {len(log_data['errors'])} 个错误")
    
    print("\n✅ 日志分析完成!")
    
    return {
        'analysis_window': time_window,
        'files_processed': len(relevant_files),
        'total_entries': log_data['total_entries'],
        'events_summary': analysis_result['events_summary'],
        'anomalies': analysis_result['anomalies'],
        'performance_issues': analysis_result['performance_issues'],
        'error_summary': analysis_result['error_summary'],
        'summary': analysis_result['summary']
    }

@tool
def analyze_hbase_metrics(
    metrics_dir: str = "hbase_metrics",
    target_time: Optional[str] = None,
    hours_range: int = 72,  # 默认3天
    metric_types: Optional[List[str]] = None
) -> dict:
    """
    清洗和处理HBase性能指标数据。纯数据处理，不提供推荐或建议。
    
    Args:
        metrics_dir: 指标数据目录路径
        target_time: 目标时间点 (可选，格式: "2025-09-12 16:00:00"，未指定则分析最近3天)
        hours_range: 分析时间范围（小时，默认72小时即3天）
        metric_types: 指标类型 (可选，如: ["handler", "wal", "queue", "gc"])
        
    Returns:
        dict: 清洗后的指标数据，包含统计信息、异常检测结果、趋势数据等
    """
    print("📈 开始分析HBase性能指标...")
    
    # 1. 加载指标数据
    metrics_data = _load_metrics_data(metrics_dir, target_time, hours_range)
    print(f"📊 加载了 {len(metrics_data)} 个节点的指标数据")
    
    # 2. 指标分析
    metrics_analysis = _analyze_metrics_comprehensive(metrics_data, metric_types)
    print(f"🔍 分析了 {len(metrics_analysis['node_analysis'])} 个节点")
    
    # 3. 数据异常统计
    anomalies = _detect_metrics_anomalies_comprehensive(metrics_data, metrics_analysis)
    print(f"📊 统计到 {len(anomalies)} 个指标异常")
    
    # 4. 性能数据分类
    bottlenecks = _identify_performance_bottlenecks(metrics_analysis, anomalies)
    
    return {
        'analysis_window': {
            'target_time': target_time,
            'hours_range': hours_range
        },
        'metrics_summary': metrics_analysis,
        'anomalies': anomalies,
        'performance_bottlenecks': bottlenecks,
        'trends': _analyze_performance_trends(metrics_data),
        'summary': _generate_metrics_summary(metrics_analysis, anomalies)
    }

# ============================================================================
# 日志分析辅助函数
# ============================================================================

def _discover_log_files(log_dir: str) -> List[str]:
    """发现日志文件"""
    log_files = []
    
    if not os.path.exists(log_dir):
        print(f"⚠️ 日志目录不存在: {log_dir}")
        return log_files
    
    for root, dirs, files in os.walk(log_dir):
        for file in files:
            # 包含所有日志文件：.log, .out, .gz, 以及带日期的文件
            if (file.endswith(('.log', '.out', '.gz')) or 
                re.search(r'\d{4}-\d{2}-\d{2}-\d{2}', file)):
                file_path = os.path.join(root, file)
                log_files.append(file_path)
    
    return log_files

def _parse_time_window(start_time: Optional[str], end_time: Optional[str]) -> dict:
    """解析时间窗口"""
    if start_time and end_time:
        start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    elif start_time:
        start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = start_dt + timedelta(hours=1)  # 默认1小时窗口
    else:
        # 默认分析最近1小时
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(hours=1)
    
    return {
        'start': start_dt,
        'end': end_dt,
        'start_str': start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        'end_str': end_dt.strftime("%Y-%m-%d %H:%M:%S")
    }

def _filter_relevant_files(log_files: List[str], time_window: dict, target_nodes: Optional[List[str]]) -> List[str]:
    """过滤相关的日志文件"""
    relevant_files = []
    
    # 提取时间窗口的日期
    target_dates = _extract_target_dates(time_window)
    print(f"   📅 目标日期: {target_dates}")
    
    # 统计节点分布
    all_nodes = set()
    date_filtered_files = []
    
    for log_file in log_files:
        node_name = _extract_node_name(log_file)
        all_nodes.add(node_name)
        
        # 检查文件是否包含目标日期
        if _is_file_relevant_for_dates(log_file, target_dates):
            date_filtered_files.append(log_file)
    
    print(f"   🌐 发现节点: {sorted(list(all_nodes))} ({len(all_nodes)} 个)")
    print(f"   📅 日期过滤后: {len(date_filtered_files)} 个文件")
    
    # 节点过滤
    if target_nodes:
        print(f"   🎯 应用节点过滤: {target_nodes}")
        for log_file in date_filtered_files:
            node_name = _extract_node_name(log_file)
            if node_name in target_nodes:
                relevant_files.append(log_file)
        print(f"   🎯 节点过滤后: {len(relevant_files)} 个文件")
    else:
        relevant_files = date_filtered_files
        print(f"   ✓ 无节点过滤，保留所有相关文件")
    
    return relevant_files

def _extract_target_dates(time_window: dict) -> List[str]:
    """从时间窗口提取目标日期列表"""
    start_date = time_window['start'].date()
    end_date = time_window['end'].date()
    
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date = datetime.combine(current_date, datetime.min.time()).date() + timedelta(days=1)
    
    return dates

def _is_file_relevant_for_dates(file_path: str, target_dates: List[str]) -> bool:
    """检查文件是否与目标日期相关"""
    filename = os.path.basename(file_path)
    
    # 检查文件名中是否包含目标日期
    for date_str in target_dates:
        if date_str in filename:
            return True
    
    # 对于当前日志文件（没有日期后缀的.log文件），也包含进来
    if filename.endswith('.log') and not re.search(r'\d{4}-\d{2}-\d{2}', filename):
        return True
    
    # 对于.out文件，也检查日期
    if filename.endswith('.out'):
        return any(date_str in filename for date_str in target_dates)
    
    return False

def _extract_node_name(file_path: str) -> str:
    """从文件路径提取节点名"""
    filename = os.path.basename(file_path)
    
    # 匹配 ip-xxx-xxx-xxx-xxx 格式（更广泛的IP格式）
    ip_match = re.search(r'ip-\d+-\d+-\d+-\d+', filename)
    if ip_match:
        return ip_match.group()
    
    # 其他格式的节点名提取
    return filename.replace('.log', '').replace('.out', '')

def _parse_log_files(log_files: List[str], time_window: dict, focus_areas: Optional[List[str]]) -> dict:
    """解析日志文件"""
    log_data = {
        'total_entries': 0,
        'events': [],
        'errors': [],
        'nodes': set()
    }
    
    timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}')
    processed_files = 0
    failed_files = 0
    
    if focus_areas:
        print(f"   🔍 关注领域: {focus_areas}")
    else:
        print(f"   🔍 分析所有领域: handler, wal, gc, memory, compaction, split, flush, queue, network, table, balancer, errors, clients, performance")
    
    for i, log_file in enumerate(log_files):
        node_name = _extract_node_name(log_file)
        log_data['nodes'].add(node_name)
        file_entries = 0
        
        try:
            file_opener = gzip.open if log_file.endswith('.gz') else open
            mode = 'rt' if log_file.endswith('.gz') else 'r'
            
            with file_opener(log_file, mode, encoding='utf-8', errors='ignore') as f:
                for line in f:
                    # 确保line是字符串类型
                    if isinstance(line, bytes):
                        line = line.decode('utf-8', errors='ignore')
                    
                    # 提取时间戳
                    timestamp_match = timestamp_pattern.search(line)
                    if not timestamp_match:
                        continue
                    
                    try:
                        log_time = datetime.strptime(timestamp_match.group(1), "%Y-%m-%d %H:%M:%S")
                        if not (time_window['start'] <= log_time <= time_window['end']):
                            continue
                    except:
                        continue
                    
                    log_data['total_entries'] += 1
                    file_entries += 1
                    
                    # 解析日志行
                    _parse_log_line(line, log_time, node_name, log_data, focus_areas)
            
            processed_files += 1
            if file_entries > 0:
                print(f"   📄 {node_name}: {file_entries} 条记录 ({os.path.basename(log_file)})")
                    
        except Exception as e:
            failed_files += 1
            print(f"   ⚠️ 处理失败 {os.path.basename(log_file)}: {e}")
    
    log_data['nodes'] = list(log_data['nodes'])
    print(f"   ✓ 成功处理: {processed_files} 个文件")
    if failed_files > 0:
        print(f"   ⚠️ 处理失败: {failed_files} 个文件")
    
    return log_data

def _parse_log_line(line: str, log_time: datetime, node_name: str, log_data: dict, focus_areas: Optional[List[str]]):
    """解析单行日志"""
    # Handler相关
    if not focus_areas or 'handler' in focus_areas:
        handler_match = re.search(r'handler=(\d+)', line)
        if handler_match:
            handler_count = int(handler_match.group(1))
            log_data['events'].append({
                'type': 'handler_usage',
                'timestamp': log_time,
                'node': node_name,
                'value': handler_count,
                'line': line.strip()
            })
    
    # WAL相关
    if not focus_areas or 'wal' in focus_areas:
        # WAL慢同步
        wal_match = re.search(r'Slow sync cost: (\d+) ms', line)
        if wal_match:
            sync_time = int(wal_match.group(1))
            log_data['events'].append({
                'type': 'wal_slow_sync',
                'timestamp': log_time,
                'node': node_name,
                'value': sync_time,
                'line': line.strip()
            })
        
        # WAL滚动
        if 'Rolling' in line and 'WAL' in line:
            log_data['events'].append({
                'type': 'wal_rolling',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
        
        # WAL大小
        wal_size_match = re.search(r'WAL.*size=(\d+)', line)
        if wal_size_match:
            wal_size = int(wal_size_match.group(1))
            log_data['events'].append({
                'type': 'wal_size',
                'timestamp': log_time,
                'node': node_name,
                'value': wal_size,
                'line': line.strip()
            })
    
    # GC相关
    if not focus_areas or 'gc' in focus_areas:
        # GC暂停
        gc_pause_match = re.search(r'GC pause (\d+)ms', line)
        if gc_pause_match:
            pause_time = int(gc_pause_match.group(1))
            log_data['events'].append({
                'type': 'gc_pause',
                'timestamp': log_time,
                'node': node_name,
                'value': pause_time,
                'line': line.strip()
            })
        
        # GC信息
        if 'GC' in line and any(gc_type in line for gc_type in ['ParNew', 'CMS', 'G1', 'Full GC']):
            log_data['events'].append({
                'type': 'gc_event',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
    
    # 内存相关
    if not focus_areas or 'memory' in focus_areas:
        # 堆内存使用
        heap_match = re.search(r'heap.*?(\d+)M/(\d+)M', line)
        if heap_match:
            used_heap = int(heap_match.group(1))
            total_heap = int(heap_match.group(2))
            log_data['events'].append({
                'type': 'heap_usage',
                'timestamp': log_time,
                'node': node_name,
                'used': used_heap,
                'total': total_heap,
                'line': line.strip()
            })
        
        # 内存不足警告
        if 'OutOfMemoryError' in line or 'low memory' in line.lower():
            log_data['events'].append({
                'type': 'memory_warning',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
    
    # 压缩相关
    if not focus_areas or 'compaction' in focus_areas:
        # 压缩开始
        if 'Starting compaction' in line:
            log_data['events'].append({
                'type': 'compaction_start',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
        
        # 压缩完成
        compaction_match = re.search(r'Completed compaction.*?(\d+)ms', line)
        if compaction_match:
            compaction_time = int(compaction_match.group(1))
            log_data['events'].append({
                'type': 'compaction_complete',
                'timestamp': log_time,
                'node': node_name,
                'value': compaction_time,
                'line': line.strip()
            })
        
        # 主压缩
        if 'major compaction' in line.lower():
            log_data['events'].append({
                'type': 'major_compaction',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
    
    # 分裂相关
    if not focus_areas or 'split' in focus_areas:
        # Region分裂
        if 'Splitting' in line and 'region' in line.lower():
            log_data['events'].append({
                'type': 'region_split',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
        
        # 分裂完成
        if 'Split' in line and 'completed' in line.lower():
            log_data['events'].append({
                'type': 'split_complete',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
    
    # 刷写相关
    if not focus_areas or 'flush' in focus_areas:
        # 刷写开始
        if 'Flushing' in line:
            log_data['events'].append({
                'type': 'flush_start',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
        
        # 刷写完成
        flush_match = re.search(r'Flushed.*?(\d+)ms', line)
        if flush_match:
            flush_time = int(flush_match.group(1))
            log_data['events'].append({
                'type': 'flush_complete',
                'timestamp': log_time,
                'node': node_name,
                'value': flush_time,
                'line': line.strip()
            })
    
    # 队列相关
    if not focus_areas or 'queue' in focus_areas:
        # RPC队列
        queue_match = re.search(r'queue=(\d+)', line)
        if queue_match:
            queue_size = int(queue_match.group(1))
            log_data['events'].append({
                'type': 'queue_size',
                'timestamp': log_time,
                'node': node_name,
                'value': queue_size,
                'line': line.strip()
            })
        
        # 队列满
        if 'queue full' in line.lower() or 'queue overflow' in line.lower():
            log_data['events'].append({
                'type': 'queue_full',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
    
    # 网络相关
    if not focus_areas or 'network' in focus_areas:
        # 网络超时
        if 'SocketTimeoutException' in line or 'Connection timeout' in line:
            log_data['events'].append({
                'type': 'network_timeout',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
        
        # 连接重置
        if 'Connection reset' in line or 'Connection refused' in line:
            log_data['events'].append({
                'type': 'connection_reset',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
    
    # 表操作相关
    if not focus_areas or 'table' in focus_areas:
        # 表创建
        if 'Creating table' in line:
            log_data['events'].append({
                'type': 'table_create',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
        
        # 表删除
        if 'Deleting table' in line:
            log_data['events'].append({
                'type': 'table_delete',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
        
        # 表访问
        table_match = re.search(r'table=([^,\s]+)', line)
        if table_match:
            table_name = table_match.group(1)
            log_data['events'].append({
                'type': 'table_access',
                'timestamp': log_time,
                'node': node_name,
                'table': table_name,
                'line': line.strip()
            })
    
    # 负载均衡相关
    if not focus_areas or 'balancer' in focus_areas:
        # 负载均衡开始
        if 'Balancer' in line and 'start' in line.lower():
            log_data['events'].append({
                'type': 'balancer_start',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
        
        # Region移动
        if 'Moving region' in line:
            log_data['events'].append({
                'type': 'region_move',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
    
    # 错误相关
    if not focus_areas or 'errors' in focus_areas:
        if any(keyword in line for keyword in ['ERROR', 'Exception', 'timed out', 'FATAL', 'WARN']):
            log_data['errors'].append({
                'timestamp': log_time,
                'node': node_name,
                'type': _classify_error(line),
                'line': line.strip()
            })
    
    # 客户端连接
    if not focus_areas or 'clients' in focus_areas:
        client_match = re.search(r'connection: ([\d\.]+):\d+', line)
        if client_match:
            client_ip = client_match.group(1)
            log_data['events'].append({
                'type': 'client_connection',
                'timestamp': log_time,
                'node': node_name,
                'client_ip': client_ip,
                'line': line.strip()
            })
        
        # 客户端断开
        if 'client disconnect' in line.lower() or 'connection closed' in line.lower():
            log_data['events'].append({
                'type': 'client_disconnect',
                'timestamp': log_time,
                'node': node_name,
                'line': line.strip()
            })
    
    # 性能相关
    if not focus_areas or 'performance' in focus_areas:
        # 慢查询
        slow_query_match = re.search(r'Slow query.*?(\d+)ms', line)
        if slow_query_match:
            query_time = int(slow_query_match.group(1))
            log_data['events'].append({
                'type': 'slow_query',
                'timestamp': log_time,
                'node': node_name,
                'value': query_time,
                'line': line.strip()
            })
        
        # 响应时间
        response_match = re.search(r'response time.*?(\d+)ms', line)
        if response_match:
            response_time = int(response_match.group(1))
            log_data['events'].append({
                'type': 'response_time',
                'timestamp': log_time,
                'node': node_name,
                'value': response_time,
                'line': line.strip()
            })

def _classify_error(line: str) -> str:
    """分类错误类型"""
    line_lower = line.lower()
    
    # 超时相关
    if 'timed out' in line or 'timeout' in line_lower:
        return 'timeout'
    
    # 内存相关
    elif 'outofmemoryerror' in line_lower or 'out of memory' in line_lower:
        return 'memory'
    
    # 网络相关
    elif any(net_err in line_lower for net_err in ['connection', 'socket', 'network']):
        return 'network'
    
    # IO相关
    elif any(io_err in line_lower for io_err in ['ioexception', 'disk', 'file']):
        return 'io'
    
    # 权限相关
    elif any(perm_err in line_lower for perm_err in ['permission', 'access denied', 'unauthorized']):
        return 'permission'
    
    # 配置相关
    elif any(conf_err in line_lower for conf_err in ['configuration', 'config', 'property']):
        return 'configuration'
    
    # 数据相关
    elif any(data_err in line_lower for data_err in ['corrupt', 'checksum', 'data']):
        return 'data'
    
    # 资源相关
    elif any(res_err in line_lower for res_err in ['resource', 'quota', 'limit']):
        return 'resource'
    
    # 级别分类
    elif 'FATAL' in line:
        return 'fatal'
    elif 'ERROR' in line:
        return 'error'
    elif 'WARN' in line:
        return 'warning'
    elif 'Exception' in line:
        return 'exception'
    else:
        return 'unknown'

def _analyze_parsed_logs(log_data: dict) -> dict:
    """分析解析后的日志数据"""
    events = log_data['events']
    errors = log_data['errors']
    
    print(f"   📊 分析 {len(events)} 个事件和 {len(errors)} 个错误...")
    
    # 统计Handler使用情况
    handler_events = [e for e in events if e['type'] == 'handler_usage']
    handler_stats = {}
    
    for event in handler_events:
        node = event['node']
        if node not in handler_stats:
            handler_stats[node] = []
        handler_stats[node].append(event['value'])
    
    if handler_events:
        print(f"   🔧 Handler事件: {len(handler_events)} 个，涉及 {len(handler_stats)} 个节点")
        for node, values in handler_stats.items():
            if values:
                max_val = max(values)
                avg_val = statistics.mean(values)
                print(f"      - {node}: 最大{max_val}, 平均{avg_val:.1f} ({len(values)}次记录)")
    
    # 检测异常
    anomalies = []
    performance_issues = []
    
    print(f"   🔍 开始数据异常统计...")
    
    # Handler数据异常统计
    handler_anomalies = 0
    for node, values in handler_stats.items():
        if values:
            max_handler = max(values)
            avg_handler = statistics.mean(values)
            
            if max_handler >= 58:  # 接近饱和
                handler_anomalies += 1
                severity = 'critical' if max_handler >= 60 else 'high'
                anomalies.append({
                    'type': 'handler_saturation',
                    'node': node,
                    'severity': severity,
                    'max_value': max_handler,
                    'avg_value': avg_handler,
                    'description': f'节点 {node} Handler数据: 最大{max_handler}, 平均{avg_handler:.1f}'
                })
                print(f"      � Handdler数据异常: {node} (最大{max_handler}, 级别:{severity})")
    
    # WAL性能问题
    wal_events = [e for e in events if e['type'] == 'wal_slow_sync']
    if wal_events:
        slow_syncs = len(wal_events)
        avg_sync_time = statistics.mean([e['value'] for e in wal_events])
        print(f"   ⏱️ WAL慢同步: {slow_syncs} 次，平均耗时 {avg_sync_time:.1f}ms")
        
        if avg_sync_time > 100:  # 超过100ms认为是慢同步
            performance_issues.append({
                'type': 'wal_performance',
                'severity': 'high',
                'slow_sync_count': slow_syncs,
                'avg_sync_time': avg_sync_time,
                'description': f'WAL同步数据: {slow_syncs}次慢同步, 平均{avg_sync_time:.1f}ms'
            })
            print(f"      � WAL同步数据:: 平均同步时间 ({avg_sync_time:.1f}ms > 100ms)")
    
    # 错误率分析
    if errors:
        error_rate = len(errors) / log_data['total_entries'] * 100 if log_data['total_entries'] > 0 else 0
        error_types = {}
        for error in errors:
            error_type = error.get('type', 'unknown')
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        print(f"   ❌ 错误分析: {len(errors)} 个错误，错误率 {error_rate:.2f}%")
        print(f"      错误类型分布: {error_types}")
        
        if error_rate > 5:  # 错误率超过5%
            anomalies.append({
                'type': 'high_error_rate',
                'severity': 'high',
                'error_count': len(errors),
                'error_rate': error_rate,
                'description': f'错误统计: {len(errors)}个错误, 错误率{error_rate:.1f}%'
            })
            print(f"      � 错误率率数据: {error_rate:.2f}% > 5%")
    
    # 事件汇总
    events_summary = _summarize_events(events)
    
    # 错误汇总
    error_summary = _summarize_errors(errors)
    
    # 生成摘要
    summary = _generate_analysis_summary(log_data, anomalies, performance_issues)
    
    print(f"   ✅ 数据统计完成: {len(anomalies)} 个异常, {len(performance_issues)} 个性能数据")
    
    return {
        'events_summary': events_summary,
        'anomalies': anomalies,
        'performance_issues': performance_issues,
        'error_summary': error_summary,
        'handler_stats': handler_stats,
        'summary': summary
    }

def _summarize_events(events: List[dict]) -> dict:
    """汇总事件信息"""
    if not events:
        return {'total': 0, 'types': {}}
    
    # 统计事件类型
    event_types = {}
    node_stats = {}
    time_distribution = {}
    
    for event in events:
        event_type = event.get('type', 'unknown')
        node = event.get('node', 'unknown')
        timestamp = event.get('timestamp')
        
        # 事件类型统计
        event_types[event_type] = event_types.get(event_type, 0) + 1
        
        # 节点统计
        if node not in node_stats:
            node_stats[node] = {}
        node_stats[node][event_type] = node_stats[node].get(event_type, 0) + 1
        
        # 时间分布（按小时）
        if timestamp:
            hour_key = timestamp.strftime('%H:00')
            time_distribution[hour_key] = time_distribution.get(hour_key, 0) + 1
    
    return {
        'total': len(events),
        'types': event_types,
        'by_node': node_stats,
        'time_distribution': time_distribution,
        'top_event_types': sorted(event_types.items(), key=lambda x: x[1], reverse=True)[:5]
    }

def _summarize_errors(errors: List[dict]) -> dict:
    """汇总错误信息"""
    if not errors:
        return {'total': 0, 'types': {}}
    
    error_types = {}
    node_stats = {}
    
    for error in errors:
        error_type = error.get('type', 'unknown')
        node = error.get('node', 'unknown')
        
        # 错误类型统计
        error_types[error_type] = error_types.get(error_type, 0) + 1
        
        # 节点统计
        if node not in node_stats:
            node_stats[node] = {}
        node_stats[node][error_type] = node_stats[node].get(error_type, 0) + 1
    
    return {
        'total': len(errors),
        'types': error_types,
        'by_node': node_stats,
        'top_error_types': sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:3]
    }

def _generate_analysis_summary(log_data: dict, anomalies: List[dict], performance_issues: List[dict]) -> str:
    """生成分析摘要"""
    summary_parts = []
    
    summary_parts.append(f"日志条目: {log_data['total_entries']}")
    summary_parts.append(f"节点数: {len(log_data['nodes'])}")
    summary_parts.append(f"异常: {len(anomalies)}")
    summary_parts.append(f"性能问题: {len(performance_issues)}")
    summary_parts.append(f"错误: {len(log_data['errors'])}")
    
    return " | ".join(summary_parts)

# ============================================================================
# 指标分析相关函数（保持原有实现）
# ============================================================================

def _load_metrics_data(metrics_dir: str, target_time: Optional[str], hours_range: int) -> dict:
    """加载指标数据"""
    if not os.path.exists(metrics_dir):
        print(f"⚠️ 指标目录不存在: {metrics_dir}")
        return {}
    
    # 确定时间范围
    if target_time:
        start_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M:%S")
        end_time = start_time + timedelta(hours=hours_range)
    else:
        # 默认分析最近3天
        end_time = datetime.now()
        start_time = end_time - timedelta(days=3)
    
    # 加载指标文件
    metrics_data = {
        'nodes': [],
        'handler_metrics': {},
        'time_range': {
            'start': start_time.strftime("%Y-%m-%d %H:%M:%S"),
            'end': end_time.strftime("%Y-%m-%d %H:%M:%S"),
            'hours': hours_range if target_time else 72
        }
    }
    
    # 尝试加载download.json文件
    download_file = os.path.join(metrics_dir, 'download.json')
    if os.path.exists(download_file):
        try:
            with open(download_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 解析时序数据
            if 'series' in data:
                for series in data['series']:
                    node_name = _extract_node_from_series_name(series.get('name', ''))
                    if not node_name:
                        continue
                    
                    # 提取时间和数值数据
                    time_field = None
                    value_field = None
                    
                    for field in series.get('fields', []):
                        if field.get('type') == 'time':
                            time_field = field
                        elif field.get('type') == 'number':
                            value_field = field
                    
                    if time_field and value_field:
                        timestamps = time_field.get('values', [])
                        values = value_field.get('values', [])
                        
                        # 转换时间戳并过滤时间范围
                        filtered_data = _filter_metrics_by_time(timestamps, values, start_time, end_time)
                        
                        if filtered_data['timestamps']:
                            metrics_data['nodes'].append(node_name)
                            metrics_data['handler_metrics'][node_name] = filtered_data
            
            print(f"📊 成功加载 {len(metrics_data['nodes'])} 个节点的指标数据")
            
        except Exception as e:
            print(f"⚠️ 加载指标文件失败: {e}")
    
    return metrics_data

def _extract_node_from_series_name(series_name: str) -> Optional[str]:
    """从series名称中提取节点名"""
    # 匹配 "Total: ip-xxx-xxx-xxx-xxx" 格式
    match = re.search(r'ip-\d+-\d+-\d+-\d+', series_name)
    return match.group() if match else None

def _filter_metrics_by_time(timestamps: list, values: list, start_time: datetime, end_time: datetime) -> dict:
    """根据时间范围过滤指标数据"""
    filtered_timestamps = []
    filtered_values = []
    
    for i, timestamp_ms in enumerate(timestamps):
        if i >= len(values):
            break
        
        # 转换毫秒时间戳为datetime
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000)
        
        # 检查是否在时间范围内
        if start_time <= timestamp_dt <= end_time:
            filtered_timestamps.append(timestamp_dt)
            filtered_values.append(values[i])
    
    return {
        'timestamps': filtered_timestamps,
        'values': filtered_values,
        'count': len(filtered_values)
    }

def _analyze_metrics_comprehensive(metrics_data: dict, metric_types: Optional[List[str]]) -> dict:
    """全面分析指标"""
    analysis = {
        'node_analysis': {},
        'cluster_summary': {},
        'performance_metrics': {}
    }
    
    if not metrics_data.get('handler_metrics'):
        return analysis
    
    # 分析每个节点的Handler指标
    node_stats = {}
    all_values = []
    
    for node_name, node_data in metrics_data['handler_metrics'].items():
        values = node_data['values']
        timestamps = node_data['timestamps']
        
        if not values:
            continue
        
        # 计算统计信息
        stats = {
            'max': max(values),
            'min': min(values),
            'mean': statistics.mean(values),
            'median': statistics.median(values),
            'std': statistics.stdev(values) if len(values) > 1 else 0,
            'count': len(values),
            'timestamps': timestamps,
            'values': values
        }
        
        # 检测高使用率时间点
        high_usage_points = []
        for i, (ts, val) in enumerate(zip(timestamps, values)):
            if val >= 50:  # Handler使用率超过50认为是高使用
                high_usage_points.append({
                    'timestamp': ts,
                    'value': val,
                    'index': i
                })
        
        stats['high_usage_points'] = high_usage_points
        stats['high_usage_count'] = len(high_usage_points)
        
        # 检测饱和情况（接近或达到60）
        saturation_points = []
        for i, (ts, val) in enumerate(zip(timestamps, values)):
            if val >= 58:  # 接近饱和
                saturation_points.append({
                    'timestamp': ts,
                    'value': val,
                    'severity': 'critical' if val >= 60 else 'high'
                })
        
        stats['saturation_points'] = saturation_points
        stats['saturation_count'] = len(saturation_points)
        
        node_stats[node_name] = stats
        all_values.extend(values)
    
    analysis['node_analysis'] = node_stats
    
    # 集群级别统计
    if all_values:
        analysis['cluster_summary'] = {
            'total_nodes': len(node_stats),
            'cluster_max': max(all_values),
            'cluster_mean': statistics.mean(all_values),
            'cluster_std': statistics.stdev(all_values) if len(all_values) > 1 else 0,
            'total_data_points': len(all_values)
        }
    
    # 性能指标汇总
    high_usage_nodes = [node for node, stats in node_stats.items() if stats['high_usage_count'] > 0]
    saturated_nodes = [node for node, stats in node_stats.items() if stats['saturation_count'] > 0]
    
    analysis['performance_metrics'] = {
        'high_usage_nodes': high_usage_nodes,
        'saturated_nodes': saturated_nodes,
        'high_usage_node_count': len(high_usage_nodes),
        'saturated_node_count': len(saturated_nodes)
    }
    
    return analysis

def _detect_metrics_anomalies_comprehensive(metrics_data: dict, metrics_analysis: dict) -> List[dict]:
    """统计指标数据异常"""
    anomalies = []
    
    node_analysis = metrics_analysis.get('node_analysis', {})
    
    for node_name, stats in node_analysis.items():
        # 1. Handler饱和异常
        if stats['saturation_count'] > 0:
            severity = 'critical' if stats['max'] >= 60 else 'high'
            anomalies.append({
                'type': 'handler_saturation',
                'node': node_name,
                'severity': severity,
                'max_value': stats['max'],
                'saturation_count': stats['saturation_count'],
                'description': f"节点 {node_name} Handler数据: 最大值{stats['max']}, {stats['saturation_count']}次达到阈值",
                'timestamps': [sp['timestamp'] for sp in stats['saturation_points']],
                'values': [sp['value'] for sp in stats['saturation_points']]
            })
        
        # 2. 持续高使用率异常
        elif stats['high_usage_count'] > 5:  # 超过5次高使用率
            anomalies.append({
                'type': 'high_handler_usage',
                'node': node_name,
                'severity': 'medium',
                'max_value': stats['max'],
                'mean_value': stats['mean'],
                'high_usage_count': stats['high_usage_count'],
                'description': f"节点 {node_name} Handler数据: 平均{stats['mean']:.1f}, {stats['high_usage_count']}次超过50",
                'timestamps': [hp['timestamp'] for hp in stats['high_usage_points']],
                'values': [hp['value'] for hp in stats['high_usage_points']]
            })
        
        # 3. Handler使用率波动异常
        if stats['std'] > 15 and stats['max'] > 30:  # 标准差大且有较高峰值
            anomalies.append({
                'type': 'handler_volatility',
                'node': node_name,
                'severity': 'medium',
                'std_value': stats['std'],
                'max_value': stats['max'],
                'description': f"节点 {node_name} Handler波动数据: 标准差{stats['std']:.1f}, 最大值{stats['max']}"
            })
    
    # 4. 集群级别异常
    cluster_summary = metrics_analysis.get('cluster_summary', {})
    performance_metrics = metrics_analysis.get('performance_metrics', {})
    
    if performance_metrics.get('saturated_node_count', 0) > 1:
        anomalies.append({
            'type': 'cluster_saturation',
            'severity': 'critical',
            'affected_nodes': performance_metrics['saturated_nodes'],
            'node_count': performance_metrics['saturated_node_count'],
            'description': f"集群Handler数据: {performance_metrics['saturated_node_count']}个节点达到阈值"
        })
    
    elif performance_metrics.get('high_usage_node_count', 0) > len(node_analysis) * 0.3:  # 超过30%节点高使用率
        anomalies.append({
            'type': 'cluster_high_usage',
            'severity': 'high',
            'affected_nodes': performance_metrics['high_usage_nodes'],
            'node_count': performance_metrics['high_usage_node_count'],
            'description': f"集群Handler数据: {performance_metrics['high_usage_node_count']}个节点高使用率"
        })
    
    return anomalies

def _identify_performance_bottlenecks(metrics_analysis: dict, anomalies: List[dict]) -> List[dict]:
    """分类性能数据"""
    bottlenecks = []
    
    # 基于异常数据分类
    critical_anomalies = [a for a in anomalies if a['severity'] == 'critical']
    high_anomalies = [a for a in anomalies if a['severity'] == 'high']
    
    if critical_anomalies:
        bottlenecks.append({
            'type': 'handler_capacity',
            'severity': 'critical',
            'description': 'Handler容量数据异常',
            'affected_components': [a.get('node', 'cluster') for a in critical_anomalies],
            'impact': 'severe_performance_degradation'
        })
    
    if high_anomalies:
        bottlenecks.append({
            'type': 'resource_pressure',
            'severity': 'high', 
            'description': 'Handler资源使用率高',
            'affected_components': [a.get('node', 'cluster') for a in high_anomalies],
            'impact': 'performance_degradation'
        })
    
    return bottlenecks

def _analyze_performance_trends(metrics_data: dict) -> dict:
    """分析性能趋势"""
    trends = {}
    
    if not metrics_data.get('handler_metrics'):
        return trends
    
    # 分析整体趋势
    all_timestamps = []
    all_values = []
    
    for node_data in metrics_data['handler_metrics'].values():
        all_timestamps.extend(node_data['timestamps'])
        all_values.extend(node_data['values'])
    
    if all_values:
        # 按时间排序
        time_value_pairs = list(zip(all_timestamps, all_values))
        time_value_pairs.sort(key=lambda x: x[0])
        
        sorted_values = [pair[1] for pair in time_value_pairs]
        
        # 计算趋势
        if len(sorted_values) >= 10:
            first_half = sorted_values[:len(sorted_values)//2]
            second_half = sorted_values[len(sorted_values)//2:]
            
            first_avg = statistics.mean(first_half)
            second_avg = statistics.mean(second_half)
            
            trend_direction = 'increasing' if second_avg > first_avg * 1.1 else \
                            'decreasing' if second_avg < first_avg * 0.9 else 'stable'
            
            trends = {
                'overall_trend': trend_direction,
                'first_half_avg': first_avg,
                'second_half_avg': second_avg,
                'trend_magnitude': abs(second_avg - first_avg) / first_avg if first_avg > 0 else 0
            }
    
    return trends

def _generate_metrics_summary(metrics_analysis: dict, anomalies: List[dict]) -> str:
    """生成指标分析摘要"""
    summary_parts = []
    
    cluster_summary = metrics_analysis.get('cluster_summary', {})
    performance_metrics = metrics_analysis.get('performance_metrics', {})
    
    # 基本统计
    if cluster_summary:
        summary_parts.append(f"节点数: {cluster_summary.get('total_nodes', 0)}")
        summary_parts.append(f"集群最大Handler: {cluster_summary.get('cluster_max', 0)}")
        summary_parts.append(f"集群平均Handler: {cluster_summary.get('cluster_mean', 0):.1f}")
    
    # 异常统计
    critical_count = len([a for a in anomalies if a['severity'] == 'critical'])
    high_count = len([a for a in anomalies if a['severity'] == 'high'])
    medium_count = len([a for a in anomalies if a['severity'] == 'medium'])
    
    if critical_count > 0:
        summary_parts.append(f"严重异常: {critical_count}")
    if high_count > 0:
        summary_parts.append(f"重要异常: {high_count}")
    if medium_count > 0:
        summary_parts.append(f"中等异常: {medium_count}")
    
    # 性能状态
    if performance_metrics:
        saturated_count = performance_metrics.get('saturated_node_count', 0)
        high_usage_count = performance_metrics.get('high_usage_node_count', 0)
        
        if saturated_count > 0:
            summary_parts.append(f"饱和节点: {saturated_count}")
        if high_usage_count > 0:
            summary_parts.append(f"高负载节点: {high_usage_count}")
    
    return " | ".join(summary_parts) if summary_parts else "无指标数据"