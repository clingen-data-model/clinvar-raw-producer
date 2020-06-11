(ns clinvar-scv.core-test
  (:require [clojure.test :refer :all]
            [clinvar-scv.core :refer :all]
            [jackdaw.data :as jd]
            [jackdaw.client :as jc]
            [jackdaw.streams.mock :as jsm]
            [jackdaw.serdes.json :as jdj]
            [cheshire.core :as json]))


(deftest bucket-test
  (testing "Ensure bucket is set to correct value"
    (is (= "broad-dsp-monster-clingen-dev-ingest-results" bucket))))

(deftest validate-datetime
  (testing "Validate datetime format"
    ; Replace with regex validating of message datetime field?
    (is (= 1 1))))

(defn mock-producer [topic-name]
  (let [test-driver jsm/build-driver
        producer (jsm/producer test-driver {:keys [topic-name jdj/serializer jdj/serializer]})]
    (producer)))

(defn consume [topic-name]
  (let [test-driver jsm/build-driver
        ;consumer (jsm/consumer test-driver {:keys [topic-name :key json/generate-string]})]
        ]
    (jsm/consume test-driver {:keys [topic-name jdj/serializer jdj/serializer]})))

(deftest test-send-update
  (testing "Test sending message to output producer"
    (let [producer mock-producer]


      )
))

;(deftest test-send-update
;  (testing "Test proper message to output producer"
;    (let [expected-key "1"
;          datetime get-release-date
;          topic "clinvar-scv-test"
;          event-type "Created"
;          event {:TestKey "TestValue"} ;"{\"TestKey\": \"TestValue\"}"
;          expected-value {:time datetime :type event-type :content (json/generate-string event)}]
;
;      (letfn [
;              (producer [{:keys [topic-name]} key value]
;                (is (= key expected-key))
;                (is (= value expected-value))
;                (is (= topic-name topic))
;                )
;              ]
;      (with-redefs-fn {#'send-update-to-exchange
;                       (fn [producer topic {:keys [key value]}]
;                         ;(tracef "Sending message to topic %s: %s:%s" topic key value)
;                         ;(println "sending message: " key)
;                         ;(jc/send! producer (jd/->ProducerRecord {:topic-name topic} key value)))
;                         (producer {:topic-name topic} key value))}
;        (send-update-to-exchange producer topic {:key   key
;                                                 :value (json/generate-string event)}))))))

