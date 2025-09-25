#!/usr/bin/env python3
"""
EMR HBase 日志下载脚本
"""

import os
import boto3
import argparse
from datetime import datetime, timedelta

def get_cluster_info_from_emr(cluster_id):
    """从EMR服务获取集群节点和IP信息"""
    emr = boto3.client('emr')
    
    print(f"从EMR服务获取集群信息: {cluster_id}")
    
    try:
        # 获取集群详情
        cluster_response = emr.describe_cluster(ClusterId=cluster_id)
        cluster = cluster_response['Cluster']
        
        print(f"集群名称: {cluster['Name']}")
        print(f"集群状态: {cluster['Status']['State']}")
        
        # 获取实例组信息
        instance_groups_response = emr.list_instance_groups(ClusterId=cluster_id)
        instance_groups = instance_groups_response['InstanceGroups']
        
        # 获取实例信息
        instances_response = emr.list_instances(ClusterId=cluster_id)
        instances = instances_response['Instances']
        
        # 建立实例组类型映射
        group_types = {}
        for group in instance_groups:
            group_types[group['Id']] = group['InstanceGroupType']
        
        node_info = {}
        
        print("节点信息:")
        for instance in instances:
            instance_id = instance['Ec2InstanceId']
            private_ip = instance['PrivateIpAddress']
            instance_group = instance['InstanceGroupId']
            group_type = group_types.get(instance_group, 'UNKNOWN')
            
            # 转换IP格式: 192.168.12.32 -> ip-192-168-12-32
            ip_formatted = f"ip-{private_ip.replace('.', '-')}"
            
            node_info[instance_id] = {
                'ip': ip_formatted,
                'group_type': group_type,
                'group_id': instance_group
            }
            
            print(f"  {instance_id} -> {private_ip} ({ip_formatted}) [{group_type}]")
        
        return node_info
        
    except Exception as e:
        print(f"获取EMR集群信息失败: {e}")
        return {}

def generate_log_paths(cluster_id, start_time, end_time, node_info):
    """根据节点信息生成对应的日志路径"""
    start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    
    paths = []
    
    print("生成路径详情:")
    print(f"  时间范围: {start_dt} ~ {end_dt}")
    
    # 计算小时数
    hours = int((end_dt - start_dt).total_seconds() / 3600) + 1
    print(f"  小时数: {hours}")
    print(f"  节点数: {len(node_info)}")
    
    current_dt = start_dt
    while current_dt <= end_dt:
        date_hour = current_dt.strftime("%Y-%m-%d-%H")
        
        for node_id, info in node_info.items():
            ip_address = info['ip']
            group_type = info['group_type']
            
            # 根据节点类型生成对应的日志
            log_types = []
            if group_type == 'MASTER':
                log_types = ["hbase-hbase-master"]
            elif group_type == 'CORE':
                log_types = ["hbase-hbase-regionserver"]
            else:
                # 如果类型未知，尝试两种都生成
                log_types = ["hbase-hbase-master", "hbase-hbase-regionserver"]
            
            for log_type in log_types:
                filename = f"{log_type}-{ip_address}.log.{date_hour}.gz"
                path = f"elasticmapreduce/{cluster_id}/node/{node_id}/applications/hbase/{filename}"
                paths.append(path)
        
        current_dt += timedelta(hours=1)
    
    print(f"  生成路径数: {len(paths)}")
    
    # 按节点类型统计
    master_paths = sum(1 for p in paths if 'hbase-master' in p)
    regionserver_paths = sum(1 for p in paths if 'regionserver' in p)
    print(f"  Master日志: {master_paths} 个")
    print(f"  RegionServer日志: {regionserver_paths} 个")
    
    return paths

def download_emr_logs(bucket, cluster_id, start_time, end_time, output_dir="hbase_log"):
    """下载EMR HBase日志"""
    s3 = boto3.client('s3')
    
    print(f"集群: {cluster_id}")
    print(f"时间: {start_time} ~ {end_time}")
    
    # 1. 从EMR服务获取节点信息
    node_info = get_cluster_info_from_emr(cluster_id)
    if not node_info:
        print("未获取到集群节点信息")
        return
    
    # 2. 生成路径
    paths = generate_log_paths(cluster_id, start_time, end_time, node_info)
    print(f"生成 {len(paths)} 个路径")
    
    # 3. 直接下载
    print(f"准备下载 {len(paths)} 个文件到 {output_dir}")
    
    # 确认下载
    if input(f"开始下载? (y/N): ").lower() not in ['y', 'yes']:
        return
    
    os.makedirs(output_dir, exist_ok=True)
    
    success = 0
    failed = 0
    failed_files = []
    
    for i, path in enumerate(paths):
        filename = os.path.basename(path)
        local_path = os.path.join(output_dir, filename)
        
        try:
            print(f"下载 {i+1}/{len(paths)}: {filename}")
            s3.download_file(bucket, path, local_path)
            success += 1
        except Exception as e:
            failed += 1
            failed_files.append(f"{filename}: {str(e)}")
            print(f"失败: {filename}")
    
    print(f"\n下载完成!")
    print(f"成功: {success} 个文件")
    print(f"失败: {failed} 个文件")
    
    if failed_files:
        print("\n失败的文件:")
        for failed_file in failed_files[:10]:  # 只显示前10个失败的
            print(f"  {failed_file}")
        if len(failed_files) > 10:
            print(f"  ... 还有 {len(failed_files) - 10} 个失败")

def main():
    parser = argparse.ArgumentParser(description='EMR HBase 日志下载')
    parser.add_argument('bucket', help='S3存储桶')
    parser.add_argument('cluster_id', help='集群ID')
    parser.add_argument('start_time', help='开始时间 "2025-08-22 06:00:00"')
    parser.add_argument('end_time', help='结束时间 "2025-08-22 18:00:00"')
    parser.add_argument('--output', default='hbase_log', help='输出目录')
    
    args = parser.parse_args()
    
    download_emr_logs(
        bucket=args.bucket,
        cluster_id=args.cluster_id,
        start_time=args.start_time,
        end_time=args.end_time,
        output_dir=args.output
    )

if __name__ == "__main__":
    main()