(ns clinvar-scv.core
  (:require [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [jackdaw.client.log :as jl])
  (:gen-class))

;(def app-config {:kafka-host (System/getenv "KAFKA_HOST")   ;;pkc-4yyd6.us-east1.gcp.confluent.cloud:9092
;                 :kafka-user (System/getenv "KAFKA_USER")   ;; CQIRWTCHFOXX6IDK
;                 :kafka-password (System/getenv "KAFKA_PASSWORD") 
;                 :kafka-topic (System/getenv "CLINVAR_SCV_TOPIC")}) ;; test

(def app-config {:kafka-host     "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                 :kafka-user     "CQIRWTCHFOXX6IDK"
                 :kafka-password "<secret here>"
                 :kafka-producer-topic    "test"
                 :kafka-consumer-topic    "broad_dsp_clinvar"})

(defn kafka-config
  "Expects, at a minimum, :user and :password in opts. "
  [opts]
  {"ssl.endpoint.identification.algorithm" "https"
   "sasl.mechanism"                        "PLAIN"
   "request.timeout.ms"                    "20000"
   "bootstrap.servers"                     (:kafka-host opts)
   "retry.backoff.ms"                      "500"
   "security.protocol"                     "SASL_SSL"
   "key.serializer"                        "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"                      "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer"                      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"                    "org.apache.kafka.common.serialization.StringDeserializer"
   "group.id"                              "dsp_clinvar_drop"
   "sasl.jaas.config"                      (str "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                                                (:kafka-user opts) "\" password=\"" (:kafka-password opts) "\";")})

(defn send-update-to-exchange [producer topic msg]
    (let [k "dsp-test-key"
          v msg]
      (println "sending message: " msg)
      (jc/send! producer (jd/->ProducerRecord {:topic-name topic} k v))))

(defn process-clinvar-drop
  "parses and processes the clinvar drop notification from dsp."
  [producer topic msg]
  (send-update-to-exchange producer topic msg))

(defn listen-for-clinvar-drop
  "listens to consumer topic for dsp clinvar drop notifications."
  [opts]
  (with-open [consumer (jc/consumer (kafka-config opts))
              producer (jc/producer (kafka-config opts))]
    (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
    (jl/log consumer)
    (while true
      (let [msgs (jc/poll consumer 100)]
        (doseq [m msgs]
          (println m)
          (process-clinvar-drop producer (:kafka-producer-topic opts) m))))))

(defn -main
  [& args]
  (listen-for-clinvar-drop app-config))
