#!/usr/bin/env python3
"""
HBase智能因果分析Agent
基于Strands SDK构建
"""
import asyncio
from strands import Agent
from hbase_tools_simplified import (
    analyze_hbase_logs,
    analyze_hbase_metrics
)
import logging
from strands.models import BedrockModel
# Configure the root strands logger
logging.getLogger("strands").setLevel(logging.DEBUG)

# Add a handler to see the logs
logging.basicConfig(
    format="%(levelname)s | %(name)s | %(message)s", 
    handlers=[logging.StreamHandler()]
)

bedrock_model = BedrockModel(
    # model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
    # region_name="us-west-2",
    additional_request_fields={
        "anthropic_beta": ["interleaved-thinking-2025-05-14"],
        "thinking": {"type": "enabled", "budget_tokens": 8000},
    },
    temperature=1,
)

MAIN_SYSTEM_PROMPT = """
你是一位资深的HBase系统诊断专家，专门负责分析和解决HBase集群的性能问题和故障。你拥有以下专业分析工具：

## 核心工具
- **hbase指标分析工具** (analyze_hbase_metrics) - 用于分析系统性能指标和异常模式
- **hbase日志分析工具** (analyze_hbase_logs) - 用于深入分析特定时间段的日志详情

## 标准诊断流程
1. **问题定位阶段**：优先使用指标分析工具识别异常时间段和影响范围
2. **根因分析阶段**：基于指标分析结果，针对性地分析相关节点在问题时间窗口内的日志
3. **结论输出阶段**：提供按优先级排序的Top 3根因分析，包含：
   - 问题描述和影响范围
   - 技术根因和证据链
   - 推荐的解决方案和预防措施

## 输出要求
- 使用结构化格式呈现分析结果
- 提供具体的时间戳、节点信息和关键指标数据
- 每个根因都要有充分的技术证据支撑
- 按照影响严重程度和发生概率对根因进行优先级排序

请基于这个框架来分析用户提出的HBase问题。
"""

def create_hbase_agent():
    """创建HBase因果分析Agent"""
    agent = Agent(
        name="HBase日志和指标分析Agent",
        system_prompt=MAIN_SYSTEM_PROMPT,
        description="专业的HBase日志解析和指标分析助手，专注于从日志中提取关键性能指标和检测异常，使用常见归因算法定位根因",
        model=bedrock_model,
        tools=[
            analyze_hbase_logs,
            analyze_hbase_metrics
        ]
    )

    return agent

def main():
    """主函数 - 演示Agent使用"""
    print("🚀 启动HBase智能因果分析Agent")
    print("=" * 50)
    
    # 创建Agent
    agent = create_hbase_agent()
    
    # 示例对话
    print("\n💬 Agent对话示例:")
    
    # 1. 分析HBase日志
    print("\n1️⃣ 用户: 请分析我的HBase日志")
    response1 = agent("9月12号hbase集群handler满了，帮我找出来top3的原因")
    print(f"🤖 Agent: {response1}")
    print(f"Total tokens: {response1.metrics.accumulated_usage['totalTokens']}")
    print(f"Input tokens: {response1.metrics.accumulated_usage['inputTokens']}")
    print(f"Output tokens: {response1.metrics.accumulated_usage['outputTokens']}")
    print(f"Execution time: {sum(response1.metrics.cycle_durations):.2f} seconds")
    print(f"Tools used: {list(response1.metrics.tool_metrics.keys())}")

async def async_example():
    """异步使用示例"""
    print("\n🔄 异步监控示例:")
    
    agent = create_hbase_agent()
    
    # 异步监控HBase指标
    response = await agent.invoke_async(
        "请监控HBase指标并检测异常"
    )
    print(f"🤖 监控结果: {response}")

def interactive_mode():
    """交互模式"""
    print("\n💬 进入交互模式 (输入 'quit' 退出)")
    print("你可以询问:")
    print("- Handler为什么打满？")
    print("- WAL为什么慢？")
    print("- 如何优化性能？")
    print("- 队列为什么积压？")
    print("-" * 40)
    
    agent = create_hbase_agent()
    
    while True:
        try:
            user_input = input("\n👤 你: ").strip()
            
            if user_input.lower() in ['quit', 'exit', '退出']:
                print("👋 再见！")
                break
            
            if not user_input:
                continue
            
            print("🤖 Agent正在分析...")
            response = agent(user_input)
            
            print(f"🤖 Agent: {response}")
            print(f"Total tokens: {response.metrics.accumulated_usage['totalTokens']}")
            print(f"Input tokens: {response.metrics.accumulated_usage['inputTokens']}")
            print(f"Output tokens: {response.metrics.accumulated_usage['outputTokens']}")
            print(f"Execution time: {sum(response.metrics.cycle_durations):.2f} seconds")
            print(f"Tools used: {list(response.metrics.tool_metrics.keys())}")
            
        except KeyboardInterrupt:
            print("\n👋 再见！")
            break
        except Exception as e:
            print(f"❌ 错误: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_mode()
    elif len(sys.argv) > 1 and sys.argv[1] == "--async":
        asyncio.run(async_example())
    else:
        main()

 
