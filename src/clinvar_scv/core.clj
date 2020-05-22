(ns clinvar-scv.core
  (:require [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [jackdaw.client.log :as jl]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [clojure.string :as s])
  (:import [com.google.cloud.storage StorageOptions BlobId]
           com.google.cloud.storage.Blob$BlobSourceOption
           java.nio.channels.Channels
           java.io.BufferedReader)
  (:gen-class))

(def app-config {:kafka-host     "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                 :kafka-user     (System/getenv "KAFKA_USER")
                 :kafka-password (System/getenv "KAFKA_PASSWORD")
                 :kafka-producer-topic    "clinvar-raw"
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

(def file-to-process "files/baseline/clinical_assertion_creates_000000000000")
(def producer-topic (:kafka-producer-topic app-config))
(def release-date "2020-05-21T20:02:00Z")

(def project "broad-dsp-monster-clingen-dev")
(def bucket "broad-dsp-monster-clingen-dev-ingest-results")

(def manifest  ["20200515T153000/clinical_assertion/creates/000000000000"
                "20200515T153000/clinical_assertion/creates/000000000001"
                "20200515T153000/clinical_assertion_observation/creates/000000000000"])

(def gc-storage (.getService (StorageOptions/getDefaultInstance)))

(defn send-update-to-exchange [producer topic {:keys [key value]}]
  (println "sending message: " key)
  (jc/send! producer (jd/->ProducerRecord {:topic-name topic} key value)))

(defn process-clinvar-drop-file
  "return a seq of parsed json messages"
  [{:keys [producer topic bucket file entity-type datetime event-type]}]

  (let [blob-id (BlobId/of bucket file)
        blob (.get gc-storage blob-id)]
    (with-open [rdr (-> blob (.reader (make-array Blob$BlobSourceOption 0)) (Channels/newReader "UTF-8") BufferedReader.) ]
      (let [lines (line-seq rdr)]
        (doseq [line (take 5 lines)]
          (let [content  (assoc (json/parse-string line true) :type entity-type)
                key (str (:id content) "_" datetime)
                event {:time datetime :type event-type :content content}]
            (println line)
            (send-update-to-exchange producer topic {:key key
                                                     :value (json/generate-string event)})))))))

(defn process-clinvar-drop
  "parses and processes the clinvar drop notification from dsp."
  [producer topic msg]

  ; 1. parse the drop message to determine where the files are
  ; this will return the folder and bucket and file manifest

  ; 2. process the folder structure in order of tables for create-update and then reverse for deletes

    ; 2a. assume we have bucket and manifest (vector of files to process with full paths in bucket.
  (doseq [file manifest]
    (let [[_ entity-type event-type _] (s/split file #"/")]
      (process-clinvar-drop-file {:producer producer
                                  :topic topic
                                  :bucket bucket
                                  :file file
                                  :entity-type entity-type
                                  :datetime release-date
                                  :event-type event-type}))))

(defn listen-for-clinvar-drop
  "listens to consumer topic for dsp clinvar drop notifications."
  [opts]
  (with-open [consumer (jc/consumer (kafka-config opts))
              producer (jc/producer (kafka-config opts))]
    (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
    (while true
      (let [msgs (jc/poll consumer 100)]
        (doseq [m msgs]
          (println m)
          (process-clinvar-drop producer (:kafka-producer-topic opts) (:value m)))))))

(defn -main
  [& args]
  (listen-for-clinvar-drop app-config))
