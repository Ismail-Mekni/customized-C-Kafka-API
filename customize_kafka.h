#ifndef CUSTOMIZE_KAFKA
#define CUSTOMIZE_KAFKA

void kafka_produce(char *brokers, char *topic, int partition, char *buf, size_t len, void (*cb)(void*));
void kafka_produce_post(char *brokers, char *topic, int partition, char *buf, size_t len);
void kafka_consume(char *brokers, char *topic, int partition, void (*f)(void*));

#endif
