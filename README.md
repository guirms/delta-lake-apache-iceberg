# Atividade Delta Lake Engenharia de dados

## Integrantes

 - [Guilherme Machado Santana](https://github.com/guirms)
 - [Jean Carlos Nesi](https://github.com/JeanNesi)
 - [Bruna Savi](https://github.com/brsavii)
 - [Kauã Librelato da Costa](https://github.com/KauaLibrelato)

## Modelo fisíco
![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/98506943/942c49a2-7ed4-44e8-93d1-5ce12128e1ea)

## Dataset utilizado
Flights 1m - https://www.tablab.app/view/parquet?datatable-source=demo-flights-1m (caso não abra com esse link utilizar esse https://www.tablab.app/datasets/sample/parquet > Flights 1m)

## Requisitos

- Python +3.6 - https://www.python.org/downloads/
- Pip(caso não venha junto com o pyhton) - https://pip.pypa.io/en/stable/installation/
- Java +8.0


# PySpark com Delta Lake

[Link do notebook](https://github.com/guirms/delta-lake-apache-iceberg/blob/main/ApacheSpark-DeltaLake.ipynb)

### 1. Como configurar o ambiente PySpark com Delta Lake
Para rodar e configurar o ambiente PySpark com Delta lake foi utilizado o [Google Colab](https://colab.research.google.com/).

#### 1.0 Criar um notebook
Primeiramente você deve criar um notebook no Colab.

#### 1.1 Dataset
Após criar o Notebook você deve realizar o download e upar o arquivo parquet do Dataset ([Flights 1m](https://www.tablab.app/datasets/sample/parquet)) para dentro do Colab.

![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/102368879/8176da93-a8bc-424d-8015-d7c6ef941db5)

#### 1.2 Instalar os pacotes

##### [pyspark](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
```py 
!pip install pyspark
```
##### [delta-spark](https://pypi.org/project/delta-spark/)
```py 
!pip install delta-spark
```

#### 1.3 Fazer as importações

```py
import pyspark
from delta import *
```


#### 1.4 Criar sessão com Pyspark e Delta Lake

```py
#  Crie uma sessão Spark com Delta
builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Obtém uma serssão spark ou cria uma nova
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Configuração para exibir apenas Logs de erro ou superior.
spark.sparkContext.setLogLevel("ERROR")
```

#### 1.5 Carregar Dataset e popular a tabela delta
```py
# Carregando dados do data frame Flights 1m.parquet
df = spark.read.parquet("/content/Flights 1m.parquet")

# Inserindo dados na tabela delta
df.write.mode(saveMode="overwrite").format("delta").save("data/delta-table")
```

#### 1.6 Visualizar tabela delta já populada
```py
# Lendo os dados
got_df = spark.read.format("delta").load("data/delta-table")
orderby_df = got_df.orderBy(got_df.FL_DATE.desc())

orderby_df.show()
```

![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/102368879/fd2679d5-2b27-47c5-a69c-9ca0eac74557)


### 2. Operações de INSERT, UPDATE e DELETE

#### 2.0 Configurando 
Precisamos criar um objeto com o caminho da tabela para conseguirmos fazer as opereções de UPDATE e DELETE

```py
from delta.tables import *
from pyspark.sql.types import DateType, ShortType, FloatType 

delta_table = DeltaTable.forPath(spark, "/content/data/delta-table")
```

#### 2.1 INSERT
```py
# Inserir dados de outro CSV
from datetime import datetime

data = [(datetime(2024, 4, 25), 12, 10, 103, 650, 17.8, 20.57845)]

schema = StructType([
    StructField("FL_DATE", DateType(), True),
    StructField("DEP_DELAY", ShortType(), True),
    StructField("ARR_DELAY", ShortType(), True),
    StructField("AIR_TIME", ShortType(), True),
    StructField("DISTANCE", ShortType(), True),
    StructField("DEP_TIME", FloatType(), True),
    StructField("ARR_TIME", FloatType(), True),
])

new_df = spark.createDataFrame(data=data, schema=schema)
new_df.write.mode(saveMode="append").format("delta").save("data/delta-table")
```

![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/102368879/8f747d95-facd-46ce-ba19-7b218988376e)


#### 2.2 UPDATE
```py
delta_table.update(
    condition="FL_DATE = '2024-04-25' AND ARR_DELAY = '10'",
    set={"ARR_DELAY": '12'}
)
```

![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/102368879/74d1c5f6-779b-4f5a-8795-0b1438beaa1c)


#### 2.3 DELETE
```py
delta_table.delete("FL_DATE = '2024-04-25'")
```

![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/102368879/fb37e2b8-11bc-4278-909a-8eec60bb2aeb)


# Apache Iceberg com Spark

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
```py
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
```py
import os
import pyspark
from pyspark.sql import SparkSession
```

### 2.2 Configurações do Apache Iceberg
Criação de variável com configurações necessárias para criar uma sessão no Spark. Entre as configurações está a declaração do pacote do Iceberg e da AWS SDK. Além disso, é configurado que o Spark utilize use o Iceberg como catálogo e especificado o nome da pasta que deve ser criada com as configurações da warehouse.
```py
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
```py
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

### 2.4 Criação da tabela com base no dataset
Criação da tabela `flights_data_1m` a partir de um arquivo *.parquet* armazenado localmente
```py
# Criar tabela com base no dataset
df = spark.read.option("header",True).parquet("./parquet-data/flights.parquet")
df.writeTo("iceberg.flights_data_1m").create()
```

### 2.5 Comando INSERT
Inserir uma nova linha na tabela `flights_data_1m`
```py
df = spark.sql("""INSERT INTO iceberg.flights_data_1m
(FL_DATE, DEP_DELAY, ARR_DELAY, AIR_TIME, DISTANCE, DEP_TIME, ARR_TIME) VALUES 
(NOW(), 7, -3, 480, 3711, 13.21, 14.2121)""")
```

### 2.5 Comando UPDATE
Atualizar uma linha da tabela `flights_data_1m` onde o valor da coluna da data do voo *FL_DATE* é maior que o dia 01/01/2024
```py
df = spark.sql("""UPDATE iceberg.flights_data_1m SET DEP_DELAY = 8 WHERE FL_DATE > '2024-01-01'""")
```

### 2.6 Comando DELETE
Atualizar uma linha da tabela `flights_data_1m` onde o valor da coluna da data do voo *FL_DATE* é maior que o dia 01/01/2024
```py
df = spark.sql("""DELETE FROM iceberg.flights_data_1m WHERE FL_DATE > '2024-01-01'""")
```

### 2.7 Comando SELECT
Lê todas as colunas das cinco primeiras linhas da tabela `flights_data_1m` onde o valor da coluna da data do voo *FL_DATE* é maior que o dia 01/01/2024
```py
df = spark.sql("SELECT * FROM iceberg.flights_data_1m WHERE FL_DATE > '2006-01-01' ORDER BY FL_DATE DESC LIMIT 5")
df.show()
```

Resultado da operação:
![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/85650237/3b12cf59-2195-4520-9836-7689b9de4e8e)



