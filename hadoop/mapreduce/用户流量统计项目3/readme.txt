1.本项目是在"用户流量统计项目1"的基础上进行升级
  （1）用户流量统计项目1需求：统计每一个用户（手机号）所耗费的总上行流量、下行流量，总流量
   (2) 本项目新增需求：除了项目1的需求外，新增按照用户总流量进行排序

2.数据源：flow.log

3.项目代码：FlowCount.java，FlowBean.java和ProvincePartitioner.java，其中函数FlowCount.java负责统计每一个用户所耗费的总上行流量、下行流量，总流量
	    FlowCountSort.java负责进行按照流量排序

4.操作日志及效果：操作日志及效果.txt

5.jar架包：FlowCount.jar和FlowCountSort.jar