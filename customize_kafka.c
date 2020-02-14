#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <time.h>
#include <sys/time.h>
#include <getopt.h>
#include <librdkafka/rdkafka.h>
#include <time.h>
#include <pthread.h>

#define MAXCHAR 10000


static int run = 1;
static rd_kafka_t *rk;
static int exit_eof = 0;
static int quiet = 0;
static void (*f)(void*);
static char *buffer;

static 	enum
{
    OUTPUT_HEXDUMP,
    OUTPUT_RAW,
} output = OUTPUT_HEXDUMP;

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    if(err==RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN)
    {
        f(buffer);
    }

    /*if (err == RD_KAFKA_RESP_ERR__FATAL)
    {
        char errstr[512];
        err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        printf("%% FATAL ERROR CALLBACK: %s: %s: %s\n",
               rd_kafka_name(rk), rd_kafka_err2str(err), errstr);
    }
    else
    {
        printf("%% ERROR CALLBACK: %s: %s: %s\n",
               rd_kafka_name(rk), rd_kafka_err2str(err), reason);
    }*/
}

static void msg_delivered (rd_kafka_t *rk,
                           const rd_kafka_message_t *rkmessage, void *opaque)
{
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                rd_kafka_err2str(rkmessage->err));
    else if (!quiet)
        fprintf(stderr,
                "%% Message delivered (%zd bytes, offset %"PRId64", "
                "partition %"PRId32"): %.*s\n",
                rkmessage->len, rkmessage->offset,
                rkmessage->partition,
                (int)rkmessage->len, (const char *)rkmessage->payload);
}

static void logger (const rd_kafka_t *rk, int level,
                    const char *fac, const char *buf)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
            (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
            level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

static void hexdump (FILE *fp, const char *name, const void *ptr, size_t len)
{
    const char *p = (const char *)ptr;
    size_t of = 0;

    if (name)
        fprintf(fp, "%s hexdump (%zd bytes):\n", name, len);

    for (of = 0 ; of < len ; of += 16)
    {
        char hexen[16*3+1];
        char charen[16+1];
        int hof = 0;

        int cof = 0;
        int i;

        for (i = of ; i < (int)of + 16 && i < (int)len ; i++)
        {
            hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
            cof += sprintf(charen+cof, "%c",
                           isprint((int)p[i]) ? p[i] : '.');
        }
        fprintf(fp, "%08zx: %-48s %-16s\n",
                of, hexen, charen);
    }
}


static void msg_consume (rd_kafka_message_t *rkmessage,
                         void *opaque)
{
    if (rkmessage->err)
    {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
        {
            fprintf(stderr,
                    "%% Consumer reached end of %s [%"PRId32"] "
                    "message queue at offset %"PRId64"\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition, rkmessage->offset);

            if (exit_eof)
                run = 0;

            return;
        }

        fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
                "offset %"PRId64": %s\n",
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition,
                rkmessage->offset,
                rd_kafka_message_errstr(rkmessage));

        if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
            run = 0;
        return;
    }

    if (!quiet)
    {
        rd_kafka_timestamp_type_t tstype;
        int64_t timestamp;
        rd_kafka_headers_t *hdrs;

        fprintf(stdout, "%% Message (offset %"PRId64", %zd bytes):\n",
                rkmessage->offset, rkmessage->len);

        timestamp = rd_kafka_message_timestamp(rkmessage, &tstype);
        if (tstype != RD_KAFKA_TIMESTAMP_NOT_AVAILABLE)
        {
            const char *tsname = "?";
            if (tstype == RD_KAFKA_TIMESTAMP_CREATE_TIME)
                tsname = "create time";
            else if (tstype == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME)
                tsname = "log append time";

            fprintf(stdout, "%% Message timestamp: %s %"PRId64
                    " (%ds ago)\n",
                    tsname, timestamp,
                    !timestamp ? 0 :
                    (int)time(NULL) - (int)(timestamp/1000));
        }

        if (!rd_kafka_message_headers(rkmessage, &hdrs))
        {
            size_t idx = 0;
            const char *name;
            const void *val;
            size_t size;

            fprintf(stdout, "%% Headers:");

            while (!rd_kafka_header_get_all(hdrs, idx++,
                                            &name, &val, &size))
            {
                fprintf(stdout, "%s%s=",
                        idx == 1 ? " " : ", ", name);
                if (val)
                    fprintf(stdout, "\"%.*s\"",
                            (int)size, (const char *)val);
                else
                    fprintf(stdout, "NULL");
            }
            fprintf(stdout, "\n");
        }
    }

    if (rkmessage->key_len)
    {
        if (output == OUTPUT_HEXDUMP)
            hexdump(stdout, "Message Key",
                    rkmessage->key, rkmessage->key_len);
        else
            printf("Key: %.*s\n",
                   (int)rkmessage->key_len, (char *)rkmessage->key);
    }

    if (output == OUTPUT_HEXDUMP)
    {
        hexdump(stdout, "Message Payload",
                rkmessage->payload, rkmessage->len);
    }
    else
        printf("%.*s\n",
               (int)rkmessage->len, (char *)rkmessage->payload);
}

void kafka_consume(char *brokers, char *topic, int partition, void (*f)(void*))
{
    void *buf;
    char errstr[512];
    size_t len;
    int i;
    int get_wmarks = 0;

    rd_kafka_conf_t *conf;
    //char buf[2048];
    char tmp[16];
    int64_t start_offset = 0;
    rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_topic_t *rkt;
    rd_kafka_headers_t *hdrs = NULL;
    rd_kafka_resp_err_t err;
    int64_t seek_offset = 0;

    conf = rd_kafka_conf_new();
    rd_kafka_conf_set_log_cb(conf, logger);
    snprintf(tmp, sizeof(tmp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();
    /*
    		 * Consumer
    		 */

    rd_kafka_conf_set(conf, "enable.partition.eof", "true",
                      NULL, 0);
    rd_kafka_conf_set(conf, "metadata.broker.list", "172.20.54.9:9093",
                      NULL, 0);
    rd_kafka_conf_set(conf, "security.protocol", "ssl",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.ca.location", "/usr/bin/NetSens/CARoot.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.certificate.location", "/usr/bin/NetSens/certificate.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.key.location", "/usr/bin/NetSens/key.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.key.password", "ismail",
                      NULL, 0);
    /* Create Kafka handle */
    if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
                            errstr, sizeof(errstr))))
    {
        fprintf(stderr,
                "%% Failed to create new consumer: %s\n",
                errstr);
        exit(1);
    }

    /* Add brokers */
    if (rd_kafka_brokers_add(rk, brokers) == 0)
    {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }

    if (get_wmarks)
    {
        int64_t lo, hi;
        rd_kafka_resp_err_t err;

        /* Only query for hi&lo partition watermarks */

        if ((err = rd_kafka_query_watermark_offsets(
                       rk, topic, partition, &lo, &hi, 5000)))
        {
            fprintf(stderr, "%% query_watermark_offsets() "
                    "failed: %s\n",
                    rd_kafka_err2str(err));
            exit(1);
        }

        printf("%s [%d]: low - high offsets: "
               "%"PRId64" - %"PRId64"\n",
               topic, partition, lo, hi);

        rd_kafka_destroy(rk);
        exit(0);
    }


    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);
    topic_conf = NULL; /* Now owned by topic */

    /* Start consuming */
    if (rd_kafka_consume_start(rkt, partition, start_offset) == -1)
    {
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        fprintf(stderr, "%% Failed to start consuming: %s\n",
                rd_kafka_err2str(err));
        if (err == RD_KAFKA_RESP_ERR__INVALID_ARG)
            fprintf(stderr,
                    "%% Broker based offset storage "
                    "requires a group.id, "
                    "add: -X group.id=yourGroup\n");
        exit(1);
    }

    while (run)
    {
        rd_kafka_message_t *rkmessage;
        rd_kafka_resp_err_t err;

        /* Poll for errors, etc. */
        rd_kafka_poll(rk, 0);

        /* Consume single message.
         * See rdkafka_performance.c for high speed
         * consuming of messages. */
        rkmessage = rd_kafka_consume(rkt, partition, 1000);
        if (!rkmessage) /* timeout */
            continue;

        (*f)(rkmessage->payload);
        msg_consume(rkmessage, NULL);

        /* Return message to rdkafka */
        rd_kafka_message_destroy(rkmessage);

        if (seek_offset)
        {
            err = rd_kafka_seek(rkt, partition, seek_offset,
                                2000);
            if (err)
                printf("Seek failed: %s\n",
                       rd_kafka_err2str(err));
            else
                printf("Seeked to %"PRId64"\n",
                       seek_offset);
            seek_offset = 0;
        }
    }

    /* Stop consuming */
    rd_kafka_consume_stop(rkt, partition);

    while (rd_kafka_outq_len(rk) > 0)
        rd_kafka_poll(rk, 10);

    /* Destroy topic */
    rd_kafka_topic_destroy(rkt);

    /* Destroy handle */
    rd_kafka_destroy(rk);
}

void kafka_produce(char *brokers, char *topic, int partition, void *buf,size_t len,void (*cb)(void*))
{
    buffer=buf;
    f=cb;
    char errstr[512];
    rd_kafka_conf_t *conf;
    int sendcnt = 0;
    char tmp[16];
    rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_topic_t *rkt;
    rd_kafka_headers_t *hdrs = NULL;
    rd_kafka_resp_err_t err;
    conf = rd_kafka_conf_new();
    rd_kafka_conf_set_error_cb(conf,err_cb);
    rd_kafka_conf_set_log_cb(conf, logger);
    snprintf(tmp, sizeof(tmp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);
    rd_kafka_conf_set(conf, "metadata.broker.list", "172.20.54.9:9093",
                      NULL, 0);
    rd_kafka_conf_set(conf, "security.protocol", "ssl",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.ca.location", "/usr/bin/NetSens/CARoot.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.certificate.location", "/usr/bin/NetSens/certificate.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.key.location", "/usr/bin/NetSens/key.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.key.password", "ismail",
                      NULL, 0);

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();
    /* Set up a message delivery report callback.
     * It will be called once for each message, either on successful
     * delivery to broker, or upon failure to deliver to broker. */
    rd_kafka_conf_set_dr_msg_cb(conf, msg_delivered);

    /* Create Kafka handle */
    if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                            errstr, sizeof(errstr))))
    {
        fprintf(stderr,
                "%% Failed to create new producer: %s\n",
                errstr);
        exit(1);
    }

    /* Add brokers */
    if (rd_kafka_brokers_add(rk, brokers) == 0)
    {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }

    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);
    topic_conf = NULL; /* Now owned by topic */

    if (!quiet)
        fprintf(stderr,
                "%% Type stuff and hit enter to send\n");

    /*len = strlen(buf);
    if (buf[len-1] == '\n')
        buf[--len] = '\0';*/

    err = RD_KAFKA_RESP_ERR_NO_ERROR;
    
    /* Send/Produce message. */
    if (hdrs)
    {
        rd_kafka_headers_t *hdrs_copy;

        hdrs_copy = rd_kafka_headers_copy(hdrs);

        err = rd_kafka_producev(
                  rk,
                  RD_KAFKA_V_RKT(rkt),
                  RD_KAFKA_V_PARTITION(partition),
                  RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                  RD_KAFKA_V_VALUE(buf, len),
                  RD_KAFKA_V_HEADERS(hdrs_copy),
                  RD_KAFKA_V_END);

        if (err)
            rd_kafka_headers_destroy(hdrs_copy);

    }
    else
    {
        if (rd_kafka_produce(
                    rkt, partition,
                    RD_KAFKA_MSG_F_COPY,
                    /* Payload and length */
                    buf, len,
                    /* Optional key and its length */
                    NULL, 0,
                    /* Message opaque, provided in
                     * delivery report callback as
                     * msg_opaque. */
                    NULL) == -1)
        {
            err = rd_kafka_last_error();
        }
    }
    
    //free(buf);

    if (err)
    {
        fprintf(stderr,
                "%% Failed to produce to topic %s "
                "partition %i: %s\n",
                rd_kafka_topic_name(rkt), partition,
                rd_kafka_err2str(err));

        /* Poll to handle delivery reports */
        rd_kafka_poll(rk, 0);
    }

    if (!quiet)
        fprintf(stderr, "%% Sent %zd bytes to topic "
                "%s partition %i\n",
                len, rd_kafka_topic_name(rkt), partition);
    sendcnt++;

    /* Poll to handle delivery reports */
    rd_kafka_poll(rk, 0);

    /* Wait for messages to be delivered */
    while (run && rd_kafka_outq_len(rk) > 0)
        rd_kafka_poll(rk, 100);

    /* Destroy topic */
    rd_kafka_topic_destroy(rkt);

    /* Destroy the handle */
    rd_kafka_destroy(rk);
}

void kafka_produce_post(char *brokers, char *topic, int partition, void *buf,size_t len)
{
    buffer=buf;
    char errstr[512];
    rd_kafka_conf_t *conf;
    int sendcnt = 0;
    char tmp[16];
    rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_topic_t *rkt;
    rd_kafka_headers_t *hdrs = NULL;
    rd_kafka_resp_err_t err;
    conf = rd_kafka_conf_new();
    rd_kafka_conf_set_log_cb(conf, logger);
    snprintf(tmp, sizeof(tmp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);
    rd_kafka_conf_set(conf, "metadata.broker.list", "172.20.54.9:9093",
                      NULL, 0);
    rd_kafka_conf_set(conf, "security.protocol", "ssl",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.ca.location", "/usr/bin/NetSens/CARoot.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.certificate.location", "/usr/bin/NetSens/certificate.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.key.location", "/usr/bin/NetSens/key.pem",
                      NULL, 0);
    rd_kafka_conf_set(conf, "ssl.key.password", "ismail",
                      NULL, 0);

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();
    /* Set up a message delivery report callback.
     * It will be called once for each message, either on successful
     * delivery to broker, or upon failure to deliver to broker. */
    rd_kafka_conf_set_dr_msg_cb(conf, msg_delivered);

    /* Create Kafka handle */
    if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                            errstr, sizeof(errstr))))
    {
        fprintf(stderr,
                "%% Failed to create new producer: %s\n",
                errstr);
        exit(1);
    }

    /* Add brokers */
    if (rd_kafka_brokers_add(rk, brokers) == 0)
    {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }

    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);
    topic_conf = NULL; /* Now owned by topic */

    if (!quiet)
        fprintf(stderr,
                "%% Type stuff and hit enter to send\n");

    /*len = strlen(buf);
    if (buf[len-1] == '\n')
        buf[--len] = '\0';*/

    err = RD_KAFKA_RESP_ERR_NO_ERROR;
    
    /* Send/Produce message. */
    if (hdrs)
    {
        rd_kafka_headers_t *hdrs_copy;

        hdrs_copy = rd_kafka_headers_copy(hdrs);

        err = rd_kafka_producev(
                  rk,
                  RD_KAFKA_V_RKT(rkt),
                  RD_KAFKA_V_PARTITION(partition),
                  RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                  RD_KAFKA_V_VALUE(buf, len),
                  RD_KAFKA_V_HEADERS(hdrs_copy),
                  RD_KAFKA_V_END);

        if (err)
            rd_kafka_headers_destroy(hdrs_copy);

    }
    else
    {
        if (rd_kafka_produce(
                    rkt, partition,
                    RD_KAFKA_MSG_F_COPY,
                    /* Payload and length */
                    buf, len,
                    /* Optional key and its length */
                    NULL, 0,
                    /* Message opaque, provided in
                     * delivery report callback as
                     * msg_opaque. */
                    NULL) == -1)
        {
            err = rd_kafka_last_error();
        }
    }
    
    //free(buf);

    if (err)
    {
        fprintf(stderr,
                "%% Failed to produce to topic %s "
                "partition %i: %s\n",
                rd_kafka_topic_name(rkt), partition,
                rd_kafka_err2str(err));

        /* Poll to handle delivery reports */
        rd_kafka_poll(rk, 0);
    }

    if (!quiet)
        fprintf(stderr, "%% Sent %zd bytes to topic "
                "%s partition %i\n",
                len, rd_kafka_topic_name(rkt), partition);
    sendcnt++;

    /* Poll to handle delivery reports */
    rd_kafka_poll(rk, 0);

    /* Wait for messages to be delivered */
    while (run && rd_kafka_outq_len(rk) > 0)
        rd_kafka_poll(rk, 100);

    /* Destroy topic */
    rd_kafka_topic_destroy(rkt);
    
}
