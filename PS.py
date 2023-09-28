# -*- coding: utf-8 -*- 
# @Time : 2023/9/28 18:04 
# @Author : Jerry Hao
# @File : PS.py 
# @Desc :

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc

# 创建一个 SparkSession
spark = SparkSession.builder.appName("WordCountExample").getOrCreate()

# 从文本文件加载数据
lines = spark.read.text("speech_117.txt")

# 切分每行文本为单词
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# 计算每个单词的词频
word_counts = words.groupBy("word").count().orderBy(desc("count"))

# 显示词频统计结果
word_counts.show()[0 : 10]

# 停止 SparkSession
spark.stop()
