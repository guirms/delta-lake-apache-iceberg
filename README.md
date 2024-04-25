# Atividade Delta Lake Engenharia de dados

## Integrantes

 - [Guilherme Machado Santana](https://github.com/guirms)
 - [Jean Carlos Nesi](https://github.com/JeanNesi)
 - [Bruna Savi](https://github.com/brsavii)
 - [Kauã Librelato da Costa](https://github.com/KauaLibrelato)

## Dataset utilizado
Flights 1m - https://www.tablab.app/view/parquet?datatable-source=demo-flights-1m (caso não abra com esse link utilizar esse https://www.tablab.app/datasets/sample/parquet > Flights 1m)

## Requisitos

- Python +3.6 - https://www.python.org/downloads/
- Pip(caso não venha junto com o pyhton) - https://pip.pypa.io/en/stable/installation/
- Java +8.0


# PySpark com Delta Lake

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
from delta.tables import *
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
print("Reading delta file ...!")

got_df = spark.read.format("delta").load("data/delta-table")
got_df.show()
```


### 2. Operações de INSERT, UPDATE e DELETE

#### 2.0 INSERT
```py

```

#### 2.1 UPDATE
```py
delta_table.update(
    condition="FL_DATE = '2006-02-15' AND ARR_TIME = '19.766666'",
    set={"ARR_DELAY": '12'}
)
```

#### 2.2 DELETE
```py
delta_table.delete("FL_DATE = '2006-02-15' AND ARR_TIME = '19.766666'")
```

Neste exemplo de DELETE, o primeiro elemento da tabela foi deletado.
##### Antes: 
![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/102368879/0ff03ca6-595c-4b88-be58-ceaf0c9daf41)

##### Depois: 
![image](https://github.com/guirms/delta-lake-apache-iceberg/assets/102368879/0680b43e-5c2e-4f9d-b11d-bc2859cb9f18)



