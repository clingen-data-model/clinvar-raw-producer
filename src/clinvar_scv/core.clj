(ns clinvar-scv.core
  (:require [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [jackdaw.client.log :as jl]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [cheshire.core :as json]
            [clojure.string :as s]
            [clojure.core :as core]
            [clojure.spec.alpha :as spec]
            [clinvar-scv.config :as cfg]
            [clinvar-scv.spec :as cspec]
            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]])
  (:import [com.google.cloud.storage Storage StorageOptions BlobId Blob]
           com.google.cloud.storage.Blob$BlobSourceOption
           java.nio.channels.Channels
           java.io.BufferedReader
           (java.util Date TimeZone)
           (java.text SimpleDateFormat)
           (java.lang Thread))
  (:gen-class))

;(def file-to-process "files/baseline/clinical_assertion_creates_000000000000")
;(def producer-topic (:kafka-producer-topic cfg/app-config))
;(def release-date "2020-05-21T20:02:00Z")
(defn get-release-date []
  (let [now (Date.)
        fmt (SimpleDateFormat. cfg/date-format)]
  (.setTimeZone fmt (TimeZone/getTimeZone "UTC"))
  (.format fmt now)))

(def messages-to-consume (atom []))

(def bucket "broad-dsp-monster-clingen-dev-ingest-results")

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
                          {:type "clinical_assertion_observation"}
                          {:type "trait_mapping"}])

(def delete-order-of-processing (reverse order-of-processing))

(def event-procedures [{:event-type :create :order order-of-processing :filter-string "created"}
                       {:event-type :update :order order-of-processing  :filter-string "updated"}
                       {:event-type :delete :order delete-order-of-processing :filter-string "deleted"}])

(def gc-storage (.getService (StorageOptions/getDefaultInstance)))


(defn send-update-to-exchange [producer topic {:keys [key value]}]
  (tracef "Sending message to topic %s: %s:%s" topic key value)
  (jc/send! producer (jd/->ProducerRecord {:topic-name topic} key value)))

(defn google-storage-line-reader [bucket filename]
  (let [blob-id (BlobId/of bucket filename)
        blob (.get gc-storage blob-id)]
    (tracef "Opening reader to blob %s/%s" bucket filename)
    ;(with-open [rdr (-> blob (.reader (make-array Blob$BlobSourceOption 0)) (Channels/newReader "UTF-8") BufferedReader.) ]
    ;  (rdr))
    (-> blob (.reader (make-array Blob$BlobSourceOption 0)) (Channels/newReader "UTF-8") BufferedReader.)
    ))

(defn line-map-to-event [line-map entity-type datetime event-type]
  "Parses a single line of a drop file, transforms into an event object map"
  (let [content (assoc line-map :entity-type entity-type)
        key (str entity-type "_" (:id content) "_" datetime)
        event {:time datetime :type event-type :content content}]
      {:key key :data event}))

(defn process-clinvar-drop-file
  "return a seq of parsed json messages"
  [{:keys [reader entity-type datetime event-type file-read-limit]
   :or {file-read-limit ##Inf}}]
  (debugf "Processing dropped file for entity-type %s and event-type %s"
          entity-type event-type)
  (let [lines (line-seq reader)]
    (debugf "Taking first %s lines" file-read-limit)

    (let [events (map #(cspec/validate %)
                      (map #(line-map-to-event % entity-type datetime event-type)
                           (map #(json/parse-string % true) (take file-read-limit lines))))]
      (doseq [event events]
        (tracef "Event message: %s" event)
        (if (not-empty (-> event :clojure.spec.alpha/problems))
          (warnf "Message for entity type %s failed spec: %s"
                 entity-type
                 (s/join "," (:clojure.spec.alpha/problems event))))
        )
      events
      )
    ))

(defn filter-files
  "Filters a collection of file strings containing a path segment which matches `filter-string`"
  [filter-string files]
  (filter #(re-find (re-pattern (str "/" filter-string "/")) % ) files ))

(defn process-clinvar-drop
  "Parses and processes the clinvar drop notification from dsp, returns a seq of output messages"
  [msg]

  ; 1. parse the drop message to determine where the files are
  ; this will return the folder and bucket and file manifest
  (debugf "File listing message: %s" msg)
  (let [parsed-drop-record (json/parse-string msg true)]
    ;; need to verify all entries in manifest are processed else warning and logging on unknown files.
    ;; 2. process the folder structure in order of tables for create-update and then reverse for deletes

      (doseq [procedure event-procedures]
        (let [files (filter-files (:filter-string procedure) (:files parsed-drop-record))]
          (doseq [record-type (:order procedure)
                  file (filter-files (:type record-type) files)]
            (let [file-reader (google-storage-line-reader bucket file)]
              (process-clinvar-drop-file {:reader       file-reader
                                          :entity-type  (:type record-type)
                                          :datetime     (:release_date parsed-drop-record)
                                          :event-type   (:event-type procedure)
                                          :filter-field (:filter record-type)})))))))

(def listen-for-drop (atom true))

(defn process-drop-messages
  "Process all outstanding messages from messages-to-consume queue"
  [opts]
  (debug "Starting process-drop-messages")
  (with-open [producer (jc/producer (cfg/kafka-config opts))]
    (while @listen-for-drop
      (when-let [msg (first @messages-to-consume)]
        (infof "Sending processed messages to output topic %s" (:kafka-producer-topic opts))
        (doseq [msg (process-clinvar-drop (:value msg))]
          (send-update-to-exchange producer (:kafka-producer-topic opts) msg))

        (swap! messages-to-consume #(into [] (rest %)))
        (Thread/sleep 100)))))

(defn listen-for-clinvar-drop
  "Listens to consumer topic for dsp clinvar drop notifications in `manifest` file list format.
  Produces output messages on producer topic."
  [opts]
  (debug "Listening for clinvar drop")
  (with-open [consumer (jc/consumer (cfg/kafka-config opts))]
    (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
    (debug "Subscribed to consumer topic " (:kafka-consumer-topic opts))
    (while true
      (let [msgs (jc/poll consumer 100)]
        (doseq [m msgs]
          (tracef "Received message: %s" m)
          (swap! messages-to-consume conj m))))))

(defn -main
  [& args]
  (.start (Thread. (partial process-drop-messages cfg/app-config)))

  (listen-for-clinvar-drop cfg/app-config))
