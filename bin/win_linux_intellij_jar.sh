# 方法1 SBT方式 
# 1 下载安装JDK
# http://www.oracle.com/technetwork/java/javase/downloads/index.html

# 2 下载安装intellij 
# http://www.jetbrains.com/idea/

# 3 安装Scala插件
# 依次选择“Configure”–> “Plugins”–> “Browse repositories”，输入scala，然后安装即可

# 4 下载Spark bin jar包
# http://spark.apache.org/downloads.html

# 5 intellij 创建空SBT项目

# 6 在intellij IDEA中创建scala project，并依次选择“File”–> “project structure” –> “Libraries”，选择“+”，
# 将spark-hadoop 对应的包导入，比如导入spark-assembly_2.10-0.9.0-incubating-hadoop2.2.0.jar
#（只需导入该jar包，其他不需要），如果IDE没有识别scala 库，则需要以同样方式将scala库导入。
# 之后开发scala程序即可
