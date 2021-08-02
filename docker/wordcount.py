import pyspark
from pyspark.sql import SparkSession

# Bibliotecas auxiliriares
import sys
import re
from pyspark.sql.functions import *


class WordCountTest():

    def __init__(self, spark: SparkSession, filesource: str, target_path: str) -> None:
        self.spark = spark
        self.filesource = filesource
        self.target_path = target_path if target_path[-1] == '/' else target_path + '/'
        self.text_file = None

    def read_file(self):
        """Carregar os dados em RDD
        """
        self.text_file = self.spark.sparkContext.textFile(self.filesource)

    def get_all_words(self):
        """Parseia o arquivo de texto limpando as palavras para aceitar apenas
           alfanumerico e letras minúsculas e conta as palavras com o reduceByKey.
        """
        return self.text_file.flatMap(lambda line: line.split(" "))\
            .map(lambda word: (re.sub("[^0-9a-zA-Z]+", "", word).lower(), 1)) \
            .reduceByKey(lambda a, b: a + b)

    def get_words_less_or_eq_then_ten_chars(self):
        """Contar as palavras que contém até 10 letras."""
        return self.text_file.flatMap(lambda line: line.split(" "))\
            .filter(lambda word: 0 < len(re.sub("[^0-9a-zA-Z]+", "", word)) <= 10)\
            .map(lambda word: (re.sub("[^0-9a-zA-Z]+", "", word).lower(), 1)) \
            .reduceByKey(lambda a, b: a + b)

    def get_words_greater_then_ten_chars(self):
        """Contar as palavras com mais de 10 letras existem no texto."""
        return self.text_file.flatMap(lambda line: line.split(" "))\
            .filter(lambda word: len(re.sub("[^0-9a-zA-Z]+", "", word)) > 10)\
            .map(lambda word: (re.sub("[^0-9a-zA-Z]+", "", word).lower(), 1)) \
            .reduceByKey(lambda a, b: a + b)

    def write_less_or_eq_then_ten_chars(self):
        """Salva um CSV contendo as palavras contadas com até ou igual 10 caracteres.
        """
        words_until_ten = self.get_words_less_or_eq_then_ten_chars()
        df = words_until_ten.toDF()\
            .withColumnRenamed('_1', 'word')\
            .withColumnRenamed('_2', 'count')
        df.coalesce(1).write.mode('overwrite').csv(
            f'{self.target_path}/words_until_ten', header=True)

    def write_greater_then_ten_chars(self):
        """Salva um CSV contendo as palavras contadas com acima de 10 caracteres.
        """
        words_greater_ten = self.get_words_greater_then_ten_chars()
        df = words_greater_ten.toDF()\
            .withColumnRenamed('_1', 'word')\
            .withColumnRenamed('_2', 'count')
        df.coalesce(1).write.mode('overwrite').csv(
            f'{self.target_path}/words_greater_then_ten', header=True)

    def run_task(self):
        """Executa o teste técnico.
        """
        self.read_file()
        self.write_less_or_eq_then_ten_chars()
        self.write_greater_then_ten_chars()


def main():
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    filesource = "data/wordcount/data.txt"
    target_path = "output/task1"

    # WordCountTest
    wct = WordCountTest(
        spark=spark, filesource=filesource, target_path=target_path)
    wct.run_task()


if __name__ == '__main__':
    main()
