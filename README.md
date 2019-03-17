# kafka-avro-demo
##### Productor y Consumidor Kafka en formato AVRO, con validación en Schema Registry de Confluent.

* Para utilizar Schema Registry de Confluent, se debe dar de alta el schema avro asociado al objeto JSON que se quiere enviar a Kafka.
* El script 'curl-schema-avro' lanza una petición POST a schema Registry para que registre el esquema asociado al tópico.
* El Schema Registry los esquemas se agrupan mediante Subjects; por defecto cada Subject tiene un esquema el cual está asociado a un tópico.
* El nombre del subject debe ser 'topico'-value. 

## Productor
* Se obtiene el esquema desde Schema Registry.
* Se serializa el objecto CalendarRecord de tipo GenericRecord pasandole el schema avro.
* Se envia al tópico el evento serializado en avro.

## Consumidor
* Se obtiene el esquema desde Schema Registry.
* Nos conectamos al tópico.
* Consumimos los eventos deserializando el objecto CalendarRecord de tipo GenericRecord pasándole el schema avro.


### Schema Registry con Docker

Accedemos a la url oficial :
https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html

Seguimos los pasos (debemos tener instalado docker-compose) :

1.- git clone https://github.com/confluentinc/cp-docker-images                                            
    git checkout 5.1.0-post


2. cd cp-docker-images/examples/cp-all-in-one/

docker-compose up -d --build

Creating network "cp-all-in-one_default" with the default driver   
Creating zookeeper ... done                                
Creating broker    ... done                             
Creating schema-registry ... done                          
Creating rest-proxy      ... done                                
Creating connect         ... done                            
Creating ksql-datagen    ... done                            
Creating ksql-server     ... done                               
Creating control-center  ... done                       
Creating ksql-cli        ... done                      


#### Operaciones

cd /cp-docker-images/examples/cp-all-in-one

Listar los contenedores: docker-compose ps

Parar los contenedores: docker-compose stop

Iniciar los contenedores: docker-compose start

Eliminar los contenedores: docker-compose down


