# Atividade Delta Lake Engenharia de dados

# Apache iceberg

## 1. Como iniciar o ambiente pré configurado
Para criar o ambiente, é necessário ter o [docker](https://www.docker.com/get-started/) instalado em sua máquina.

### 1.1 Criar instância do Jupyter com os arquivos do Apache Iceberg e Apache Spark já configurados
> Para criar essa instância você deve estar com o docker sendo executado em sua máquina local.

Execute o seguinte comando em seu terminal para baixar uma imagem do ambiente do Apache Iceberg já configurado com o Jupyter Notebook
```
docker run -p 8888:8888 --name iceberg-notebook guirms/iceberg-notebook:1.0
```
### 1.2 Acessar o notebook
Ao criar o container com o código fornecido acima, você deverá acessar o último link exibido em seu terminal
![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/85650237/34f7f043-ac2d-47c1-ac13-0ab3add4e6ed)

### 1.3 Acessar o arquivo `main.ipynb`
Ao acessar o Jupyter Notebook, acesse o arquivo `main.ipynb` clicando duas vezes sobre ele
![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/85650237/645699af-b5a0-4195-b32f-9a6f83f1907d)

### 1.3 Código completo `main.ipynb`
```
import os
import pyspark
from pyspark.sql import SparkSession

# Configurações do iceberg
conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
  		# Pacotes
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-  
     3.3_2.12:1.4.3,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
  		# Extensões do SQL
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
  		# Configurações do catalog
        .set('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.iceberg.type', 'hadoop')
        .set('spark.sql.catalog.iceberg.warehouse', 'iceberg-warehouse')
        .set("spark.sql.catalogImplementation","hive")
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

# Criar tabela com base no dataset
#df = spark.read.option("header",True).parquet("./parquet-data/flights.parquet")
#df.count()
#df.writeTo("iceberg.flights_data_1m").create()

# Inserir
#df = spark.sql("""INSERT INTO iceberg.flights_data_1m
#(FL_DATE, DEP_DELAY, ARR_DELAY, AIR_TIME, DISTANCE, DEP_TIME, ARR_TIME) VALUES 
#(NOW(), 7, -3, 480, 3711, 13.21, 14.2121)""")

# Atualizar
#df = spark.sql("""UPDATE iceberg.flights_data_1m SET DEP_DELAY = 8 WHERE FL_DATE > '2024-01-01'""")

# Deletar
#df = spark.sql("""DELETE FROM iceberg.flights_data_1m WHERE FL_DATE > '2024-01-01' AND DEP_DELAY = 8""")

# ler
df = spark.sql("SELECT * FROM iceberg.flights_data_1m WHERE FL_DATE > '2024-01-01' ORDER BY FL_DATE DESC LIMIT 5")

df.show()
```


## 2. Entendendo o código

### 2.1 Importações
Importações necessárias para que o programa funcione
```
import os
import pyspark
from pyspark.sql import SparkSession
```

### 2.2 Configurações do Apache Iceberg
Criação de variável com configurações necessárias para criar uma sessão no Spark. Entre as configurações está a declaração do pacote do Iceberg e da AWS SDK. Além disso, é configurado que o Spark utilize use o Iceberg como catálogo e especificado o nome da pasta que deve ser criada com as configurações da warehouse.
```
conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
  		# Pacotes
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-  
     3.3_2.12:1.4.3,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
  		# Extensões do SQL
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
  		# Configurações do catalog
        .set('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.iceberg.type', 'hadoop')
        .set('spark.sql.catalog.iceberg.warehouse', 'iceberg-warehouse')
        .set("spark.sql.catalogImplementation","hive")
)
```
### 2.3 Criação da sessão Spark
Com base nas configurações criadas anteriormente, é criada uma instância Spark *caso ainda não exista*
```
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

### 2.4 Criação da tabela com base no dataset
Criação da tabela `flights_data_1m` a partir de um arquivo *.parquet* armazenado localmente
```
# Criar tabela com base no dataset
df = spark.read.option("header",True).parquet("./parquet-data/flights.parquet")
df.writeTo("iceberg.flights_data_1m").create()
```

### 2.5 Comando INSERT
Inserir uma nova linha na tabela `flights_data_1m`
```
df = spark.sql("""INSERT INTO iceberg.flights_data_1m
(FL_DATE, DEP_DELAY, ARR_DELAY, AIR_TIME, DISTANCE, DEP_TIME, ARR_TIME) VALUES 
(NOW(), 7, -3, 480, 3711, 13.21, 14.2121)""")
```

### 2.5 Comando UPDATE
Atualizar uma linha da tabela `flights_data_1m` onde o valor da coluna da data do voo *FL_DATE* é maior que o dia 01/01/2024
```
df = spark.sql("""UPDATE iceberg.flights_data_1m SET DEP_DELAY = 8 WHERE FL_DATE > '2024-01-01'""")
```

### 2.6 Comando DELETE
Atualizar uma linha da tabela `flights_data_1m` onde o valor da coluna da data do voo *FL_DATE* é maior que o dia 01/01/2024
```
df = spark.sql("""DELETE FROM iceberg.flights_data_1m WHERE FL_DATE > '2024-01-01'""")
```

### 2.7 Comando SELECT
Lê todas as colunas das cinco primeiras linhas da tabela `flights_data_1m` onde o valor da coluna da data do voo *FL_DATE* é maior que o dia 01/01/2024
```
df = spark.sql("SELECT * FROM iceberg.flights_data_1m WHERE FL_DATE > '2006-01-01' ORDER BY FL_DATE DESC LIMIT 5")
df.show()
```

Resultado da operação:
![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/85650237/3b12cf59-2195-4520-9836-7689b9de4e8e)











