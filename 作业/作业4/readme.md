# 作业4 README

## MatrixMultiply 说明

### 运行说明

用于计算两个矩阵的乘积。

将`MatrixMultiply.java`源码打包成jar包后，在系统节点上执行如下命令：

> $ bin/hadoop jar MatrixMultiply.jar <Matrix M input path\> <Matrix N input path\> <output path\>

矩阵文件名为`M_<row>_<col>`和`N_<row>_<col>`

## RelationAlgebra 说明

关系代数算法下一共有6个子算法，选择，投影，并，交，差，自然连接。

**除自然连接算法外其他算法都用到了RelationA.java文件，需要一并打包为jar包并运行**

自然连接算法只需打包`NeturalJoin.java`源码为jar包进行运行。

测试文件：

* 关系Ra：(id, name, age, weight)
* 关系Rb：(id, gender, height)

### 运行说明

* 选择 Selection

  以Ra.txt作为输入文件，选择年龄等于18岁的数据（选择年龄小于18岁的仅给出运行截图，在源代码中作为注释）

> hadoop jar Selection.jar <input path\> <output path\> 

* 投影 Projection
  以Ra.txt作为输入文件，做name的投影

> hadoop jar Projection.jar <input path\> <output path\> 

* 并 UnionSet

  以Ra1.txt，Ra2.txt作为输入文件，求并集

  <input path1\>, <input path2\>为两个文件的路径

> hadoop jar UnionSet.jar <input path1\> <input path2\> <output path\> 

* 交 Intersection

  以Ra1.txt，Ra2.txt作为输入文件，求交集

  <input path1\>, <input path2\>为两个文件的路径

> hadoop jar Intersection.jar <input path1\> <input path2\> <output path\> 

* 差 Difference

  以Ra1.txt和Ra2.txt为输入文件，求Ra2-Ra1的差集

  <input path1\>, <input path2\>为两个文件的路径

> hadoop jar Difference.jar <input path1\> <input path2\> <output path\>

* 自然连接 NaturalJoin

  以Ra.txt和Rb.txt为输入文件，Ra和Rb在属性id上进行自然连接，并使输出顺序为(id, name, age, gender, weight, height)
  打包后运行：

  <input path1\>, <input path2\>为两个文件的路径

> hadoop jar NaturalJoin.jar <input path1\> <input path2\> <output path\>
