# data_engineer_challenge


*  Qual o objetivo do comando cache em Spark?

Acelerar o acesso das informações em um RDD, sendo que por padrão o mesmo executa opeações lazy não armazenando informações na memória.


*  O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

O spark é construido e utilizado com o objetivo de ser o mais eficiente possivel, por isso o mesmo possui a vantagem de trabalhar na memoria ram por ser mais eficiente na leitura das informações, no caso do mapreduce o mesmo trabalha somente em disco, afetando a eficiência do processo.


*  Qual é a função do SparkContext ?

O SparkContext representa a conexão com o cluster spark, trabalhando como um cliente, com o mesmo é possivel criar RDD, startar jobs etc.

*  Explique com suas palavras o que é Resilient Distributed Datasets (RDD)

RDD são abstrações de dados no spark, Desmistificando: Resilient por serem tolerantes à falha, Distributed pelo fato ser possivel distribuir RDDs com segurança entre nós e executar operações neles, os RDD são também lazy ou seja uma ação precisa ser chamada no final do pipeline para obter uma saída, os RDD suportam operações de transformações e ações além de serem imutaveis, após criar um RDD não é possivel altera-lo após realizado uma tranformação e ação é criado um novo RDD.

*  GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
  
  O GroupByKey envia todas as todas informações pela rede para os jobs de redução isso pode ocasionar falhas e lentidão no processo perdendo assim eficiencia, o reduceByKey combina as informações em cada partição em seguinda envia somente uma chave de cada partição pela rede 
  
  
  
*  Explique o que o código Scala abaixo faz.
val textFile = sc . textFile ( "hdfs://..." )
val counts = textFile . flatMap ( line => line . split ( " " ))
. map ( word => ( word , 1 ))
. reduceByKey ( _ + _ )
counts . saveAsTextFile ( "hdfs://..." )

O codigo é um contador de palavras, onde o mesmo realiza um mapeamento do arquivo de input encontrado dentro do HDFS, quebrando cada linha por espaco usando o split e em seguinda para cada palavra encontrada o mesmo utiliza como chave e o numero 1 como valor, quando chega no reducer o mesmo conta quantas chaves identicas existe e soma os valores.

  
