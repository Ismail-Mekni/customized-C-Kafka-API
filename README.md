# C Kafka client customization

This a simple header file and source code file that defines two functions to consume messages from Kafka and produce messages to Kafka with C programming language.
If you want to implement a Kafka client using C programming language, you will find that the only way is using librdkafka C library. Librdkafka is an advanced C library to create Kafka clients, with many features and options. After working for some days with it I've found that people don't like to go in-depth with details when it comes to Kafka producing and consuming. In general, we just need two simple functions for producing messages to Kafka and consuming messages from it.

So I came up with the idea to customize librdkafka utilities.

***NOTE: this source code is developed with librdkafka-0.11.6.***

With this simple effort, I have illustrated these two functions as described below:

```sh
void kafka_produce(char *brokers, char *topic, int partition, char *buf, size_t len, void (*cb)(void*));
```
This function is used to produce messages to Kafka, we have the fields as follows:
| Argument | Description |
| ------ | ------ |
| brokers | Kafka brokers, servers, address |
| topic | Kafka topic to produce to |
| partition | Kafka topic partition to produce to |
| buf | The message to be sent |
| len | The length of the message to be sent |
| cb | The callback function to call in the case of errors |


```sh
void kafka_consume(char *brokers, char *topic, int partition, void (*f)(void*));
```

This function is used to consume messages from Kafka, we have the fields as follows:

| Argument | Description |
| ------ | ------ |
| brokers | Kafka brokers, servers, address |
| topic | Kafka topic to consume from |
| partition | Kafka topic partition to consume from |
| f | The callback function to call when a message is consumed, it takes the message as an argument |

To start using these C files, the header file, and the source code file, you need to install librdkafka on your environment, you can find the full description [Here].

If you have any issue don't hesitate to open it or you could text me at ismmekni@gmail.com.

[Here]: https://github.com/edenhill/librdkafka
