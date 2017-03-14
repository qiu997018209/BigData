本项目是在前一个项目:reduce端实现用户订单合并的基础上实现的
需求：在前一个项目的基础上解决数据顷斜的问题，即：
      分区的时候是按照hashcode%reduce task,但是实际情况下，很可能某种订单的数量很多
      这会导致某一个reduce task上需要出来的数据非常多，存在数据顷斜的可能

      本项目采用重写Mapper中的setup方法，在创建Mapper前将产品信息文件读取到hash表中
      在map端实现订单表和产品表合并的目的

1.订单文件:order.txt,产品文件:pdts.txt
2.读取cache中的文件时,hadoop报错如下：
[root@mini1 hadoop]# hadoop jar mjoin.jar cn.qiujiahao.bigdata.mjoin.mjoin /input /test/output16
17/03/14 21:21:43 INFO client.RMProxy: Connecting to ResourceManager at mini1/192.168.199.3:8032
17/03/14 21:21:44 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
17/03/14 21:21:44 INFO input.FileInputFormat: Total input paths to process : 1
17/03/14 21:21:45 INFO mapreduce.JobSubmitter: number of splits:1
17/03/14 21:21:45 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1488757973844_0024
17/03/14 21:21:45 INFO impl.YarnClientImpl: Submitted application application_1488757973844_0024
17/03/14 21:21:46 INFO mapreduce.Job: The url to track the job: http://mini1:8088/proxy/application_1488757973844_0024/
17/03/14 21:21:46 INFO mapreduce.Job: Running job: job_1488757973844_0024
17/03/14 21:21:49 INFO mapreduce.Job: Job job_1488757973844_0024 running in uber mode : false
17/03/14 21:21:49 INFO mapreduce.Job:  map 0% reduce 0%
17/03/14 21:21:49 INFO mapreduce.Job: Job job_1488757973844_0024 failed with state FAILED due to: Application application_1488757973844_0024 failed 2 times due to AM Container for appattempt_1488757973844_0024_000002 exited with  exitCode: -1000
For more detailed output, check application tracking page:http://mini1:8088/proxy/application_1488757973844_0024/Then, click on links to logs of each attempt.
Diagnostics: File file:/home/hadoop/pdts.txt does not exist
java.io.FileNotFoundException: File file:/home/hadoop/pdts.txt does not exist
        at org.apache.hadoop.fs.RawLocalFileSystem.deprecatedGetFileStatus(RawLocalFileSystem.java:537)
        at org.apache.hadoop.fs.RawLocalFileSystem.getFileLinkStatusInternal(RawLocalFileSystem.java:750)
        at org.apache.hadoop.fs.RawLocalFileSystem.getFileStatus(RawLocalFileSystem.java:527)
        at org.apache.hadoop.fs.FilterFileSystem.getFileStatus(FilterFileSystem.java:409)
        at org.apache.hadoop.yarn.util.FSDownload.copy(FSDownload.java:251)
        at org.apache.hadoop.yarn.util.FSDownload.access$000(FSDownload.java:61)
        at org.apache.hadoop.yarn.util.FSDownload$2.run(FSDownload.java:359)
  
  3.结论:待解决
    