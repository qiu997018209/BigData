1.需求：将以下两个以文件行式存储在HDFS里的数据合并成一个信息表单

	order.txt(订单id, 日期, 商品编号, 数量)
		  1001	20150710	P0001	2
		  1002	20150710	P0002	3
		  1003	20150710	P0003	3
	product.txt(商品编号, 商品名字, 价格, 数量)
		    P0001	小米5	1001	2
		    P0002	锤子T1	1000	3
		    P0003	锤子T2	1002	4

2.在reduce端实现

3.数据源：order.txt和product.txt

4.项目代码：RJoin.java和InfoBean.java，函数入口为RJoin.java

5.效果展示：效果演示.txt

6.jar架包：rjoin.jar
