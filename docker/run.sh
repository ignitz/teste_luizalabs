# Task 1
# Leia o arquivo de texto *wordcount.txt*, e conte as palavras que contém até 10 letras.
# Conte também quantas palavras com mais de 10 letras existem no texto.
# Dataset: https://storage.googleapis.com/luizalabs-hiring-test/wordcount.txt

# SOURCE_PREFIX=data/wordcount
# TARGET_PREFIX=output

# mkdir -pv $SOURCE_PREFIX && curl -o $SOURCE_PREFIX/data.txt https://storage.googleapis.com/luizalabs-hiring-test/wordcount.txt

# # Investigando o conteúdo dos dados
# head -n 1 $SOURCE_PREFIX/data.txt

# # Remove variables on my notebook
# unset PYSPARK_DRIVER_PYTHON
# unset PYSPARK_DRIVER_PYTHON_OPTS
# spark-submit /app/wordcount.py

# echo "-----------------------------------------------------------------"
# echo "| Task 1 DONE!"
# echo "-----------------------------------------------------------------"

# Task 2
# Leia o arquivo pedidos.csv e agrupe todos os cliente que fizeram mais de 2 compras nos
# dias de black friday dos últimos três anos. Filtre todos os clientes que são menores de
# 30 anos e coloque numa lista TODOS os códigos de pedido e a data em que foram efetuados.
# Adicione também a idade do cliente.
# Dataset: https://storage.googleapis.com/luizalabs-hiring-test/clientes_pedidos.csv
# > Nota: Os dados utilizados no desafio são antigos, portanto pode considerar os últimos 5 anos e não 3 como fala no teste 2.


SOURCE_PREFIX=data/pedidos
TARGET_PREFIX=output

mkdir -pv $SOURCE_PREFIX && curl -o $SOURCE_PREFIX/data.csv https://storage.googleapis.com/luizalabs-hiring-test/clientes_pedidos.csv

# Remove variables on my notebook
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS
spark-submit /app/pedidos.py

echo "-----------------------------------------------------------------"
echo "| Task 2 DONE!"
echo "-----------------------------------------------------------------"
