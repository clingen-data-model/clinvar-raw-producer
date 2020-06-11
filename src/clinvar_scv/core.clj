(ns clinvar-scv.core
  (:require [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [jackdaw.client.log :as jl]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [clojure.string :as s]
            [clojure.core :as core]
            [clinvar-scv.config :as cfg]
            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]])
  (:import [com.google.cloud.storage Storage StorageOptions BlobId Blob]
           com.google.cloud.storage.Blob$BlobSourceOption
           java.nio.channels.Channels
           java.io.BufferedReader
           (java.util Date TimeZone)
           (java.text SimpleDateFormat))
  (:gen-class))


;(def file-to-process "files/baseline/clinical_assertion_creates_000000000000")
;(def producer-topic (:kafka-producer-topic cfg/app-config))
;(def release-date "2020-05-21T20:02:00Z")
(defn get-release-date []
  (let [now (Date.)
        fmt (SimpleDateFormat. cfg/date-format)]
  (.setTimeZone fmt (TimeZone/getTimeZone "UTC"))
  (.format fmt now)))

;(def project "broad-dsp-monster-clingen-dev")
(def bucket "broad-dsp-monster-clingen-dev-ingest-results")

; Example manifest vector
;(def manifest  ["20200515T153000/clinical_assertion/creates/000000000000"
;                "20200515T153000/clinical_assertion/creates/000000000001"
;                "20200515T153000/clinical_assertion_observation/creates/000000000000"])

(def gc-storage (.getService (StorageOptions/getDefaultInstance)))

(defn send-update-to-exchange [producer topic {:keys [key value]}]
  (tracef "Sending message to topic %s: %s:%s" topic key value)
  ;(println "sending message: " key)
  (jc/send! producer (jd/->ProducerRecord {:topic-name topic} key value)))

(defn process-clinvar-drop-file
  "return a seq of parsed json messages"
  [{:keys [producer topic bucket file entity-type datetime event-type]}]
  (debugf "Processing dropped file %s for entity-type %s and event-type %s"
          file entity-type event-type)
  (let [blob-id (BlobId/of bucket file)
        blob (.get gc-storage blob-id)]
    (tracef "Opening reader to blob %s/%s" bucket file)
    (with-open [rdr (-> blob (.reader (make-array Blob$BlobSourceOption 0)) (Channels/newReader "UTF-8") BufferedReader.) ]
      (let [lines (line-seq rdr)]
        (doseq [line (take 5 lines)]
          (tracef "Parsing file line %s" line)
          (let [content (assoc (json/parse-string line true) :type entity-type)
                key (str (:id content) "_" datetime)
                event {:time datetime :type event-type :content content}]

            (send-update-to-exchange producer topic {:key   key
                                                     :value (json/generate-string event)})))))))

(defn process-clinvar-drop
  "parses and processes the clinvar drop notification from dsp."
  [producer producer-topic file-listing]

  ; 1. parse the drop message to determine where the files are
  ; this will return the folder and bucket and file manifest
  (debugf "File listing: %s" (s/join "," file-listing))
  ; 2. process the folder structure in order of tables for create-update and then reverse for deletes

    ; 2a. assume we have bucket and manifest (vector of files to process with full paths in bucket.
  (doseq [file file-listing]
    (let [[_ entity-type event-type _] (s/split file #"/")]
      (process-clinvar-drop-file {:producer producer
                                  :topic producer-topic
                                  :bucket bucket
                                  :file file
                                  :entity-type entity-type
                                  :datetime get-release-date
                                  :event-type event-type}))))

(defn listen-for-clinvar-drop
  "Listens to consumer topic for dsp clinvar drop notifications in `manifest` file list format.
  Produces output messages on producer topic."
  [opts]
  (debugf "Opening consumer and producer kafka streams")
  (with-open [consumer (jc/consumer (cfg/kafka-config opts))
              producer (jc/producer (cfg/kafka-config opts))]
    (debugf "Subscribing to consumer topic %s" (:kafka-consumer-topic opts))
    (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
    (while true
      (let [msgs (jc/poll consumer 100)]
        (doseq [m msgs]
          (infof "Received message %s\n" m)
          ; The message contents is within the ConsumerRecord as :value
          ; The value is just a list of file paths, no additional processing needed here
          (process-clinvar-drop producer (:kafka-producer-topic opts) (:value m)))))))

(defn -main
  [& args]
  (listen-for-clinvar-drop cfg/app-config))
