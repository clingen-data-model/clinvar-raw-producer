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

(def messages-to-consume (atom []))

(def app-config {:kafka-host     "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                 :kafka-user     (System/getenv "KAFKA_USER")
                 :kafka-password (System/getenv "KAFKA_PASSWORD")
                 :kafka-producer-topic    "clinvar-raw"
                 :kafka-consumer-topic    "broad-dsp-clinvar"})

(defn kafka-config
  "Expects, at a minimum, :user and :password in opts. "
  [opts]
  {"ssl.endpoint.identification.algorithm" "https"
   "compression.type"                      "gzip"
   "sasl.mechanism"                        "PLAIN"
   "request.timeout.ms"                    "20000"
   "bootstrap.servers"                     (:kafka-host opts)
   "retry.backoff.ms"                      "500"
   "security.protocol"                     "SASL_SSL"
   "key.serializer"                        "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"                      "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer"                      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"                    "org.apache.kafka.common.serialization.StringDeserializer"
   "group.id"                              "dsp_clinvar_producer"
   "sasl.jaas.config"                      (str "org.apache.kafka.common.security.plain.PlainLoginModule required username="\"
                                                (:kafka-user opts) \"" password="\" (:kafka-password opts) \"";")})

;; (def file-to-process "files/baseline/clinical_assertion_creates_000000000000")
(def producer-topic (:kafka-producer-topic app-config))
;; (def release-date "2020-05-21T20:02:00Z")

(def project "broad-dsp-monster-clingen-dev")
(def bucket "broad-dsp-monster-clingen-dev-ingest-results")

;; (def manifest  ["20200515T153000/clinical_assertion/creates/000000000000",
;;                 "20200515T153000/clinical_assertion/creates/000000000001",
;;                 "20200515T153000/clinical_assertion_observation/creates/000000000000",
;;                 "20200515T153000/clinical_assertion_trait/creates/000000000000",
;;                 "20200515T153000/clinical_assertion_trait_set/creates/000000000000",
;;                 "20200515T153000/clinical_assertion_variation/creates/000000000000",
;;                 "20200515T153000/gene/creates/000000000000",
;;                 "20200515T153000/gene_association/creates/000000000000",
;;                 "20200515T153000/gene_association/creates/000000000001",
;;                 "20200515T153000/gene_association/creates/000000000002",
;;                 "20200515T153000/rcv_accession/creates/000000000000",
;;                 "20200515T153000/submission/creates/000000000000",
;;                 "20200515T153000/submitter/creates/000000000000",
;;                 "20200515T153000/trait/creates/000000000000",
;;                 "20200515T153000/trait_mapping/creates/000000000000",
;;                 "20200515T153000/trait_set/creates/000000000000",
;;                 "20200515T153000/variation/creates/000000000000",
;;                 "20200515T153000/variation/creates/000000000001",
;;                 "20200515T153000/variation/creates/000000000002",
;;                 "20200515T153000/variation/creates/000000000003",
;;                 "20200515T153000/variation/creates/000000000004",
;;                 "20200515T153000/variation_archive/creates/000000000000"])


(def order-of-processing [{:type "gene"}
                          {:type "variation" :filter {:field :subclass_type :value "SimpleAllele"}}
                          {:type "variation" :filter {:field :subclass_type :value "Haplotype"}}
                          {:type "variation" :filter {:field :subclass_type :value "Genotype"}}
                          {:type "gene_association"}
                          {:type "variation_archive"}
                          {:type "trait"}
                          {:type "trait_set"}
                          {:type "rcv_accession"}
                          {:type "submitter"}
                          {:type "submission"}
                          {:type "clinical_assertion"}
                          {:type "clinical_assertion_variation" :filter {:field :subclass_type :value "SimpleAllele"}}
                          {:type "clinical_assertion_variation" :filter {:field :subclass_type :value "Haplotype"}}
                          {:type "clinical_assertion_variation" :filter {:field :subclass_type :value "Genotype"}}
                          {:type "clinical_assertion_trait"}
                          {:type "clinical_assertion_trait_set"}
                          {:type "trait_mapping"}])

(def delete-order-of-processing (reverse order-of-processing))

(def event-procedures [{:event-type :create :order order-of-processing :filter-string "created"}
                       {:event-type :update :order order-of-processing  :filter-string "updated"}
                       {:event-type :delete :order delete-order-of-processing :filter-string "deleted"}])

(def gc-storage (.getService (StorageOptions/getDefaultInstance)))

(defn send-update-to-exchange [producer topic {:keys [key value]}]
  (jc/send! producer (jd/->ProducerRecord {:topic-name topic} key value)))

(defn process-clinvar-drop-file
  "return a seq of parsed json messages"
  [{:keys [producer topic bucket file entity-type datetime event-type filter-field]}]
  (let [blob-id (BlobId/of bucket file)
        blob (.get gc-storage blob-id)]
    (println file)
    (if blob
      (with-open [rdr (-> blob (.reader (make-array Blob$BlobSourceOption 0)) (Channels/newReader "UTF-8") BufferedReader.) ]
        (let [lines (map #(assoc (json/parse-string % true) :type entity-type) (line-seq rdr))
              records (if filter-field
                        (filter #(= (:value filter-field) (get % (:field filter-field))) lines)
                        lines)]
          (doseq [content (take 5 records)]
            (let [key (str (:id content) "_" datetime)
                  event {:time datetime :type event-type :content content}]
              (send-update-to-exchange producer topic {:key key
                                                       :value (json/generate-string event)})))))
      (println "file not found"))))

(defn filter-files
  [filter-string files]
  (filter #(re-find (re-pattern (str "/" filter-string "/")) % ) files ))

(defn process-clinvar-drop
  "parses and processes the clinvar drop notification from dsp."
  [producer topic msg]

  ; 1. parse the drop message to determine where the files are
  ; this will return the folder and bucket and file manifest
  ;... pull out manifest here

  (let [parsed-drop-record (json/parse-string msg true)]

    ;; need to verify all entries in manifest are processed else warning and logging on unknown files.


    ;; 2. process the folder structure in order of tables for create-update and then reverse for deletes
    (doseq [procedure event-procedures]
      (let [files (filter-files (:filter-string procedure) (:files parsed-drop-record))]

        (doseq [record-type (:order procedure)
                file (filter-files (:type record-type) files )]
          (process-clinvar-drop-file {:producer producer
                                      :topic topic
                                      :bucket bucket
                                      :file file
                                      :entity-type (:type record-type)
                                      :datetime (:release_date parsed-drop-record)
                                      :event-type (:event-type procedure)
                                      :filter-field (:filter record-type)}))))))


(def listen-for-drop (atom true))

(defn process-drop-messages
  [opts]
  (with-open [producer (jc/producer (kafka-config opts))]
    (while @listen-for-drop
      (when-let [msg (first @messages-to-consume)]
        (process-clinvar-drop producer (:kafka-producer-topic opts) (:value msg))
        (swap! messages-to-consume #(into [] (rest %)))
        (Thread/sleep 100)))))

(defn listen-for-clinvar-drop
  "listens to consumer topic for dsp clinvar drop notifications."
  [opts]
  (println "listening for clinvar drop")
  (with-open [consumer (jc/consumer (kafka-config opts))]
    (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
    (while @listen-for-drop
      (let [msgs (jc/poll consumer 100)]
        (doseq [m msgs]
          (println m)
          (swap! messages-to-consume conj m))))))

(defn -main
  [& args]
  (.start (Thread. (partial process-drop-messages app-config)))
  (listen-for-clinvar-drop app-config))
