import pyspark
from pyspark.sql import SparkSession, DataFrame

# Bibliotecas auxiliriares
import sys
import re
from pyspark.sql.functions import *


class PedidosTest():

    def __init__(self, spark: SparkSession, filesource: str, target_path: str) -> None:
        self.spark = spark
        self.filesource = filesource
        self.target_path = target_path if target_path[-1] == '/' else target_path + '/'
        self.df = None
        self.df_initial = None

    def read_file(self):
        """Carregar os dados em DataFrame
        """
        self.df_initial = self.spark.read.csv(self.filesource, header=True)
        self.df = self.df_initial.withColumn('data_nascimento_cliente', col('data_nascimento_cliente').cast('timestamp'))\
            .withColumn('data_pedido', (col('data_pedido') * 1).cast('timestamp'))\
            .withColumn('idade', floor(datediff(current_date(), 'data_nascimento_cliente')/365))

    def get_anwser_of_test(self) -> DataFrame:
        """Agrupa e filtra os dados com idade menor de 30 anos e numero de pedidos acima de 2.

        Returns:
            pyspark.sql.DataFrame: DataFrame com o resultado do teste.
        """
        return self.df.groupBy(['codigo_cliente', 'idade'])\
            .agg(count(lit(1)).alias("numero_pedidos"), collect_list(struct('codigo_pedido', to_date(col('data_pedido')).alias('data_pedido')))
                 .alias('lista_pedidos'))\
            .filter('idade < 30 and numero_pedidos > 2')

    def write_anwer_to_csv(self):
        df_aux = self.get_anwser_of_test()
        convertStructToArrayUDF = udf(lambda z: str(
            [[x[0], str(x[1])] for x in z]), StringType())

        df_aux.withColumn('lista_pedidos', convertStructToArrayUDF(col(
            'lista_pedidos'))).write.mode('overwrite').csv(f'{self.target_path}/result/temp', header=True)

        df_csv = self.spark.read.csv(
            f'{self.target_path}/result/temp', header=True)
        df_csv.coalesce(1).write.mode('overwrite').csv(
            f'{self.target_path}/result/result_one_file', header=True)

    def run_task(self):
        """Executa o teste técnico.
        """
        self.read_file()
        self.write_anwer_to_csv()


def main():
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    filesource = "data/pedidos/data.csv"
    target_path = "output/task2"

    # WordCountTest
    ped = PedidosTest(
        spark=spark, filesource=filesource, target_path=target_path)
    ped.run_task()


if __name__ == '__main__':
    main()
