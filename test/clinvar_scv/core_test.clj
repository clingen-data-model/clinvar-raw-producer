(ns clinvar-scv.core-test
  (:require [clojure.test :refer :all]
            [clinvar-scv.core :refer :all]
            [cheshire.core :as json]
            [clinvar-scv.config :as cfg])
  )

(deftest bucket-test
  (testing "Ensure bucket is set to correct value"
    (is (= "broad-dsp-monster-clingen-dev-ingest-results" bucket))))

;(deftest validate-datetime
;  (testing "Validate datetime format"
;    ; Replace with regex validating of message datetime field?
;    (is (= 1 1))))

(def clinical-assertion-line "{\"id\":\"SCV000924344\",\"version\":\"1\",\"internal_id\":\"1807657\",\"variation_archive_id\":\"VCV000634922\",\"variation_id\":\"634922\",\"submitter_id\":\"3\",\"submission_id\":\"3.2019-06-18\",\"rcv_accession_id\":\"RCV000785779\",\"trait_set_id\":\"47782\",\"clinical_assertion_trait_set_id\":\"SCV000924344\",\"clinical_assertion_observation_ids\":[\"SCV000924344.0\"],\"title\":\"NUP214, ASP154GLY_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9\",\"local_key\":\"114350.0001_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9\",\"assertion_type\":\"variation to disease\",\"date_created\":\"2019-06-20\",\"date_last_updated\":\"2019-06-24\",\"record_status\":\"current\",\"review_status\":\"no assertion criteria provided\",\"interpretation_description\":\"risk factor\",\"interpretation_date_last_evaluated\":\"2019-06-18\",\"submission_names\":[],\"interpretation_comments\":[]}")

(deftest test-line-to-event
  (testing "Test processing a line from a dropped JSON file"
    (let [line clinical-assertion-line
          entity-type "clinical_assertion"
          datetime "2020-01-01T12:00:00Z"
          event-type "create"
          expected-value {:key "SCV000924344_2020-01-01T12:00:00Z", :value "{\"time\":\"2020-01-01T12:00:00Z\",\"type\":\"create\",\"content\":{\"variation_id\":\"634922\",\"variation_archive_id\":\"VCV000634922\",\"submitter_id\":\"3\",\"date_last_updated\":\"2019-06-24\",\"interpretation_comments\":[],\"interpretation_description\":\"risk factor\",\"trait_set_id\":\"47782\",\"internal_id\":\"1807657\",\"type\":\"clinical_assertion\",\"submission_id\":\"3.2019-06-18\",\"local_key\":\"114350.0001_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9\",\"clinical_assertion_observation_ids\":[\"SCV000924344.0\"],\"title\":\"NUP214, ASP154GLY_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9\",\"assertion_type\":\"variation to disease\",\"rcv_accession_id\":\"RCV000785779\",\"clinical_assertion_trait_set_id\":\"SCV000924344\",\"id\":\"SCV000924344\",\"submission_names\":[],\"record_status\":\"current\",\"date_created\":\"2019-06-20\",\"review_status\":\"no assertion criteria provided\",\"interpretation_date_last_evaluated\":\"2019-06-18\",\"version\":\"1\"}}"}]
      (is (= expected-value (line-to-event line entity-type datetime event-type))))
))
